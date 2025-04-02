import socket
import struct
import threading
import time
import uuid
import pickle
import os
import random
import sys
import argparse
from collections import deque

# --- Configuracoes ---
MULTICAST_GROUP = '224.1.1.1'  # endereço IP multicast padrao
MULTICAST_PORT = 5007  # porta padrao para o grupo
REPLICATION_FACTOR = 3  # numero de arquivos de replica locais por cliente
REPLICATION_DIR = "replicas"  # diretorio para guardar arquivos de replica
CHECKPOINT_DIR = "checkpoints"  # diretorio para guardar arquivos de checkpoint
CHECKPOINT_INTERVAL_S = 60  # intervalo em segundos para salvar checkpoints
MESSAGE_DELAY_MS = 50  # delay maximo artificial (ms) para simular desordem
BUFFER_SIZE = 1024  # Tamanho do buffer para recebimento de mensagens

''' --- Tipos de Mensagem (formato: TIPO|REMETENTE|DADOS...) ---
# MSG|SENDER_ID|MESSAGE_CONTENT  (mensagem de chat normal)
# REQ|SENDER_ID|SEQ_NUM          (requisicao de exclusao mutua - Ricart-Agrawala)
# REP|SENDER_ID|TARGET_ID        (resposta a requisicao de exclusao mutua)
# CHK|SENDER_ID|FILENAME|CONTENT (para reconciliacao avancada) '''


class chat_client:
    def __init__(self, client_id):
        self.client_id = client_id
        # --- Estado do Chat ---
        self.messages = deque(maxlen=200)  # fila para exibir ultimas mensagens
        self.message_log = []  # log completo de mensagens para persistencia

        # --- Rede ---
        self.sock_send = None  # socket para enviar mensagens multicast
        self.sock_recv = None  # socket para receber mensagens multicast

        # --- Estado para Ricart-Agrawala (Exclusao Mutua) ---
        self.sequence_number = 0  # numero proprio de sequencia para requisicoes
        self.highest_sequence_number = 0  # maior numero de sequencia visto na rede
        self.requesting_cs = False  # se esta tentando obter acesso exclusivo
        self.outstanding_replies = set()  # conjunto de peers dos quais espera resposta (REP)
        self.deferred_replies = set()  # conjunto de peers cujas requisiçoes (REQ) adiaram
        self.peers = set()  # conjunto de ids de clientes detectados na rede
        self.mutex = threading.Lock()  # lock para proteger acesso concorrente a dados compartilhados
        self.got_all_replies_event = threading.Event()  # evento para sinalizar recebimento de todas as reps

        # --- Persistencia e Replicacao ---
        self.checkpoint_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{self.client_id}.pkl")
        self.replica_files = [
            os.path.join(REPLICATION_DIR, f"replica_{self.client_id}_{i}.log")
            for i in range(REPLICATION_FACTOR)
        ]

        # --- Controle de Exibicao e Threads ---
        self.verbose_mode = False  # exibe mensagens de sistema/debug
        self.first_prompt = True  # exibir o prompt detalhado inicial
        self.receive_thread = None  # thread para escutar mensagens da rede
        self.checkpoint_thread = None  # thread para salvar checkpoints periodicamente
        self.reconcile_thread = None  # thread para verificar consistencia local periodicamente
        self.running = True  # para controlar a execucao das threads

        # --- Inicializacao ---
        self._setup_directories()  # cria os diretorios de replicas e checkpoints
        self._load_checkpoint()  # carrega o estado anterior de um checkpoint
        self._setup_sockets()  # configura os sockets de envio e recebimento
        self.peers.add(self.client_id)  # adiciona a si mesmo a lista de peers

    # --- Funcoes Auxiliares de Logging e Prompt ---

    def _log_verbose(self, message):
        # Imprime a mensagem apenas se o modo verbose estiver ativo
        if self.verbose_mode:
            print(f"\n[VERBOSE] {message}")  # imprime em nova linha
            self._redisplay_prompt()  # redesenha o prompt

    def _log_error(self, message):
        # Imprime mensagens de erro sempre
        print(f"\n[ERRO] {message}")  # imprime em nova linha
        self._redisplay_prompt()  # redesenha o prompt

    def _log_info(self, message):
        # Imprime mensagens informativas
        print(f"\n[INFO] {message}")  # imprime em nova linha
        self._redisplay_prompt()  # redesenha o prompt

    def _redisplay_prompt(self):
        # Redesenha o prompt de input na linha atual
        if self.first_prompt:
            prompt_text = '> Digite sua mensagem (OBS. comandos reservados: "sair", "peers", "sync", "verbose"): '
            self.first_prompt = False  # so mostra o prompt completo uma vez
        else:
            prompt_text = '> '
        # \r move o cursor para o inicio da linha, permitindo sobrescrever
        # end='' evita a nova linha automatica do print
        print(f'\r{prompt_text}', end='')
        sys.stdout.flush()  # força a escrita imediata para o terminal

    # --- Configuracao Inicial ---

    def _setup_directories(self):
        # Cria os diretorios para replicas e checkpoints se nao existirem
        os.makedirs(REPLICATION_DIR, exist_ok=True)
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    def _setup_sockets(self):
        # Configura os sockets UDP para envio e recebimento multicast
        try:
            # socket para envio
            self.sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # define o Time-To-Live (TTL) dos pacotes multicast
            self.sock_send.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

            # socket para recebimento
            self.sock_recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # permite que outros sockets na mesma maquina se liguem a mesma porta necessario para varios clientes locais
            self.sock_recv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # liga o socket a todas as interfaces de rede ('') na porta definida
            self.sock_recv.bind(('', MULTICAST_PORT))

            # adiciona o socket ao grupo multicast para que ele receba as mensagens enviadas para esse grupo
            mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
            self.sock_recv.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        except OSError as e:
            # caso a porta ja esteja em uso por outro processo
            self._log_error(f"Falha ao configurar sockets na porta {MULTICAST_PORT}: {e}. Verifique se ja esta em uso.")
            sys.exit(1)
        except Exception as e:
            # erros comuns
            self._log_error(f"Erro inesperado ao configurar sockets: {e}")
            sys.exit(1)

    # --- Comunicacao e Processamento de Mensagens ---

    def _send_multicast(self, message):
        # envia uma mensagem em formato string para o grupo multicast
        try:
            # DEBUG self._log_verbose(f"Enviando: {message}")
            self.sock_send.sendto(message.encode('utf-8'), (MULTICAST_GROUP, MULTICAST_PORT))
        except Exception as e:
            # nao para a execucao caso haja um erro
            self._log_error(f"Falha ao enviar mensagem multicast: {e}")

    def _receive_messages(self):
        # loop principal da thread que escuta e processa mensagens recebidas
        self._log_verbose("Thread de recebimento iniciada.")
        while self.running:
            try:
                # espera receber dados
                data, addr = self.sock_recv.recvfrom(BUFFER_SIZE)
                message = data.decode('utf-8')
                # DEBUG self._log_verbose(f"Recebido raw: {message} de {addr}")
                self._process_incoming_message(message)
            except socket.timeout:
                # se o socket tiver timeout apenas continua
                continue
            except OSError as e:
                # ocorre se o socket for fechado enquanto recvfrom espera, ex: ao sair
                if self.running:  # so reporta erro se nao estivermos parando intencionalmente
                    self._log_error(f"Problema no socket de recebimento: {e}")
                break  # sai do loop
            except Exception as e:
                self._log_error(f"Falha ao processar mensagem recebida: {e}")
                # continua tentando receber outras mensagens
        self._log_verbose("Thread de recebimento terminada.")

    def _process_incoming_message(self, message):
        # Analisa o tipo da mensagem recebida e chama o handler apropriado
        try:
            parts = message.split('|', 2)  # divide em TIPO | REMETENTE | DADOS
            if len(parts) < 2:
                self._log_verbose(f"Formato de mensagem invalido: {message}")
                return

            msg_type = parts[0]
            sender_id = parts[1]

            # --- Descoberta de Peers (implicita) ---
            if sender_id != self.client_id:
                with self.mutex:  # protege acesso ao conjunto de peers
                    if sender_id not in self.peers:
                        self.peers.add(sender_id)
                        self._log_verbose(f"Novo acesso detectado: {sender_id}")

            # --- Roteamento da Mensagem ---
            if msg_type == "MSG" and len(parts) == 3:
                # simula delay para consistencia eventual
                delay = random.randint(0, MESSAGE_DELAY_MS) / 1000.0
                time.sleep(delay)
                content = parts[2]
                self._handle_chat_message(sender_id, content)
            elif msg_type == "REQ" and len(parts) == 3:
                try:
                    req_seq_num = int(parts[2])
                    self._handle_request_cs(sender_id, req_seq_num)
                except ValueError:
                    self._log_verbose(f"Numero de sequencia invalido em REQ: {parts[2]}")
            elif msg_type == "REP" and len(parts) == 3:
                target_id = parts[2]
                # so processa a resposta se for para mim
                if target_id == self.client_id:
                    self._handle_reply_cs(sender_id)
            # DEBUG else: Ignora tipos desconhecidos ou formatos invAlidos silenciosamente
            # DEBUG     self._log_verbose(f"Tipo/formato de mensagem nAo tratado: {message}")

        except Exception as e:
            # captura qualquer erro inesperado no processamento da mensagem
            self._log_error(f"Erro inesperado ao processar mensagem '{message}': {e}")

    def _handle_chat_message(self, sender_id, content):
        # Processa uma mensagem de chat: exibe, loga e replica
        timestamp = time.strftime('%H:%M:%S')  # hora local formatada
        display_msg = f"[{timestamp}] {sender_id}: {content}"
        # log com timestamp float para ordenaçao mais precisa
        log_entry = (sender_id, content, time.time())

        with self.mutex:  # Protege acesso as listas de mensagens
            self.messages.append(display_msg)  # adiciona a fila de exibicao
            self.message_log.append(log_entry)  # adiciona ao log completo

        # --- Exibe a mensagem de chat (SEMPRE VISIVEL) ---
        # \r move para o inicio da linha, espaço extra limpa restos do prompt antigo
        print(f"\r{display_msg}{' ' * 20}")
        self._redisplay_prompt()  # redesenha o prompt na linha seguinte

        # --- Replicacao Local (silenciosa por padrao) ---
        self._replicate_message(log_entry)

    # --- Replicacao e Consistencia ---

    def _replicate_message(self, log_entry):
        # Escreve a entrada do log em multiplos arquivos de replica locais
        sender, content, ts = log_entry
        # Formato simples para o arquivo de log: timestamp|remetente|conteudo
        log_line = f"{ts}|{sender}|{content}\n"
        for replica_file in self.replica_files:
            try:
                # 'a' para append (adicionar ao fim do arquivo)
                with open(replica_file, "a", encoding='utf-8') as f:
                    f.write(log_line)
            except Exception as e:
                # Reporta erro mas continua tentando outras replicas
                self._log_error(f"Falha ao escrever na replica {replica_file}: {e}")

    def _check_local_consistency(self):
        # Verifica se os arquivos de replica locais contem o mesmo conjunto de mensagens
        # Loga detalhes apenas em modo verbose. Retorna True se consistente, False caso contrario.

        self._log_verbose("Verificando consistencia local das replicas...")
        replica_contents = []  # lista para guardar o conjunto de linhas de cada replica
        all_lines = set()  # conjunto com todas as linhas unicas encontradas em todas as replicas
        consistent = True  # assume consistencia inicial

        try:
            # le todas as replicas e popula os conjuntos
            for idx, replica_file in enumerate(self.replica_files):
                lines = set()
                if os.path.exists(replica_file):
                    with open(replica_file, "r", encoding='utf-8') as f:
                        for line in f:
                            cleaned_line = line.strip()
                            if cleaned_line:  # ignora linhas vazias
                                lines.add(cleaned_line)
                                all_lines.add(cleaned_line)
                replica_contents.append(lines)

            if not replica_contents:
                self._log_verbose("Nenhuma replica encontrada para verificaçao.")
                return True  # Considera consistente se nao ha arquivos

            # Compara cada replica com o conjunto agregado de todas as linhas
            for i, content_set in enumerate(replica_contents):
                if content_set != all_lines:  # Se uma replica difere do total, ha inconsistencia
                    consistent = False
                    # Loga detalhes apenas se verbose
                    missing = all_lines - content_set  # Linhas que faltam nesta replica
                    extra = content_set - all_lines  # Linhas que so existem nesta replica (invalido/erro)
                    if missing:
                        self._log_verbose(
                            f" Replica {i} ({os.path.basename(self.replica_files[i])}) faltam {len(missing)} linhas.")
                    if extra:
                        self._log_verbose(
                            f" Replica {i} ({os.path.basename(self.replica_files[i])}) tem {len(extra)} linhas extras ou invalidas.")
                    # Numa implementaçao real, aqui poderia haver logica para corrigir as replicas

            # Log final do resultado
            if consistent:
                self._log_verbose("Replicas locais parecem consistentes.")
            else:
                self._log_verbose("Inconsistencia detectada nas replicas locais.")
            return consistent

        except Exception as e:
            self._log_error(f"Falha ao verificar consistencia das replicas: {e}")
            return False  # Retorna inconsistente em caso de erro

    def _periodic_reconciler(self):
        # Thread que chama a verificaçao de consistencia local periodicamente
        self._log_verbose("Thread Reconciliadora (local) iniciada.")
        while self.running:
            # Intervalo mais longo para nao poluir logs verbose
            time.sleep(120)  # Verifica a cada 2 minutos
            if self.running:
                with self.mutex:  # Garante acesso seguro aos dados, se necessario
                    # A verificaçao atual le arquivos, talvez nao precise do mutex aqui
                    # Mas se fosse reconciliar/escrever, seria essencial
                    self._check_local_consistency()
        self._log_verbose("Thread Reconciliadora (local) terminada.")

    # --- Exclusao Mutua Distribuida (Ricart-Agrawala) ---

    def _request_access(self):
        # Tenta obter acesso exclusivo para enviar uma mensagem
        with self.mutex:  # Protege acesso as variaveis de estado de R-A
            self.requesting_cs = True
            # Incrementa o numero de sequencia (Lamport timestamp para R-A)
            self.sequence_number = self.highest_sequence_number + 1
            current_request_seq = self.sequence_number  # Guarda o seq desta requisiçao

            # Define de quem esperar respostas (todos os outros peers conhecidos)
            self.outstanding_replies = {p for p in self.peers if p != self.client_id}

            self.got_all_replies_event.clear()  # Reseta o evento de espera

        # Se nao ha outros peers, considera acesso concedido imediatamente
        if not self.outstanding_replies:
            self._log_verbose("MUTEX: Nenhum outro usuario detectado. Acesso imediato.")
            # Nao precisa sinalizar evento pois a verificaçao eh sincrona
            return True

        # Envia a requisiçao para todos no grupo
        self._log_verbose(
            f"MUTEX: Solicitando acesso (Seq={current_request_seq}) para usuario: {self.outstanding_replies}")
        msg = f"REQ|{self.client_id}|{current_request_seq}"
        self._send_multicast(msg)

        # Espera pelo evento ser sinalizado (em _handle_reply_cs) com timeout
        self._log_verbose("MUTEX: Aguardando permissoes...")
        # Timeout para evitar bloqueio indefinido se um peer falhar sem responder
        got_it = self.got_all_replies_event.wait(timeout=15.0)  # 15 segundos

        if not got_it:
            # Timeout ocorreu antes de receber todas as respostas
            self._log_verbose("MUTEX: Timeout ao esperar permissoes. Abortando envio.")
            with self.mutex:
                # Limpa o estado da requisiçao para evitar problemas futuros
                self.requesting_cs = False
                # Libera peers que podem ter sido adiados durante a tentativa falha
                self._release_deferred()
            return False

        # Todas as permissoes recebidas
        self._log_verbose("MUTEX: Permissao concedida!")
        return True

    def _handle_request_cs(self, sender_id, req_seq_num):
        # Processa uma requisiçao de acesso (REQ) recebida de outro peer
        with self.mutex:  # Protege acesso as variaveis de estado R-A
            # Atualiza o maior numero de sequencia visto (Timestamp de Lamport)
            self.highest_sequence_number = max(self.highest_sequence_number, req_seq_num)

            # Logica de decisao de Ricart-Agrawala:
            # Adiar a resposta (nao enviar REP agora) se:
            # 1. Eu ja estou na seçao critica (nao implementado explicitamente, mas implicito por `requesting_cs` e espera)
            # 2. Eu estou tentando entrar na seçao critica (`requesting_cs == True`) E
            #    a minha requisiçao tem prioridade maior que a do remetente.
            #    Prioridade maior = (menor numero de sequencia) OU (mesmo numero de sequencia e menor ID de cliente)
            defer = False
            if self.requesting_cs:
                my_seq = self.sequence_number  # Seq da minha requisiçao pendente
                # Compara minha prioridade com a do remetente
                if my_seq < req_seq_num or (my_seq == req_seq_num and self.client_id < sender_id):
                    # Minha requisiçao tem prioridade, entao adio a resposta para ele
                    defer = True

            if defer:
                self._log_verbose(f"MUTEX: Adiando REP para {sender_id} (Minha Req={my_seq} vs Req Dele={req_seq_num})")
                self.deferred_replies.add(sender_id)  # Adiciona a lista de respostas a enviar depois
            else:
                # Envia a resposta (REP) imediatamente (nao estou pedindo ou ele tem prioridade)
                self._log_verbose(f"MUTEX: Enviando REP para {sender_id} (Nao peço ou ele tem prioridade)")
                reply_msg = f"REP|{self.client_id}|{sender_id}"  # TARGET_ID e quem pediu
                self._send_multicast(reply_msg)

    def _handle_reply_cs(self, sender_id):
        # Processa uma resposta de permissao (REP) recebida.
        with self.mutex:  # Protege acesso a outstanding_replies
            # Verifica se ainda estou esperando resposta e se veio de um peer esperado
            if self.requesting_cs and sender_id in self.outstanding_replies:
                self._log_verbose(f"MUTEX: Permissao (REP) recebida de {sender_id}")
                self.outstanding_replies.remove(sender_id)
                self._log_verbose(f"MUTEX: Permissoes restantes: {len(self.outstanding_replies)}")

                # Se nao ha mais respostas pendentes, sinaliza o evento para liberar a thread principal
                if not self.outstanding_replies:
                    self.got_all_replies_event.set()

    def _release_access(self):
        # Libera o acesso a seçao critica e envia respostas adiadas
        with self.mutex:  # Protege acesso a requesting_cs e deferred_replies
            self.requesting_cs = False  # Marca que nao estou mais (tentando) na seçao critica
            self._release_deferred()  # Envia as respostas que foram adiadas

    def _release_deferred(self):
        # Envia mensagens REP para todos os peers na lista de adiados.
        # Esta funçao deve ser chamada DENTRO de um bloco `with self.mutex:`
        if self.deferred_replies:
            self._log_verbose(f"MUTEX: Liberando acesso. Enviando REPs adiados para: {self.deferred_replies}")
            # Itera sobre uma copia para poder modificar o conjunto original
            for target_id in list(self.deferred_replies):
                reply_msg = f"REP|{self.client_id}|{target_id}"
                self._send_multicast(reply_msg)
            self.deferred_replies.clear()  # Limpa a lista de adiados

    # --- Tolerancia a Falhas (Checkpoints) ---

    def _save_checkpoint(self):
        # Salva o estado relevante do cliente em um arquivo pickle.
        state_to_save = {}  # Dicionario para guardar o estado
        with self.mutex:  # Garante que o estado nao mude durante a copia
            state_to_save = {
                'client_id': self.client_id,
                'message_log': list(self.message_log),  # Salva o log completo de mensagens
                'sequence_number': self.sequence_number,  # ultimo numero de sequencia usado/visto
                'highest_sequence_number': self.highest_sequence_number,
                'peers': list(self.peers)  # Lista de peers conhecidos
                # Nao salva estados volateis como outstanding_replies, deferred_replies, requesting_cs
            }
        try:
            # 'wb' = Write Bytes (necessario para pickle)
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(state_to_save, f)  # Serializa e salva o dicionario
            self._log_verbose(f"CHECKPOINT: Estado salvo em {self.checkpoint_file}")
        except Exception as e:
            self._log_error(f"Falha ao salvar checkpoint: {e}")

    def _load_checkpoint(self):
        # Carrega o estado do cliente do arquivo de checkpoint, se existir e for valido.
        if os.path.exists(self.checkpoint_file):
            try:
                # 'rb' = Read Bytes
                with open(self.checkpoint_file, 'rb') as f:
                    state = pickle.load(f)  # Carrega e desserializa o dicionario

                # Validaçao basica: pertence a este cliente?
                if state.get('client_id') == self.client_id:
                    # Restaura o estado
                    self.message_log = state.get('message_log', [])
                    self.sequence_number = state.get('sequence_number', 0)
                    self.highest_sequence_number = state.get('highest_sequence_number', 0)
                    loaded_peers = state.get('peers', [])
                    self.peers = set(loaded_peers)
                    self.peers.add(self.client_id)  # Garante que o proprio ID esta presente

                    # Preenche a deque de exibiçao com base no log restaurado
                    self.messages.clear()
                    num_restored = 0
                    for sender, content, ts in self.message_log:
                        timestamp_str = time.strftime('%H:%M:%S', time.localtime(ts))
                        self.messages.append(f"[{timestamp_str}] {sender}: {content}")
                        num_restored += 1

                    # exibe mensagens restauradas
                    if num_restored > 0:
                        # itera sobre uma copia da deque para evitar problemas de modificacao
                        for msg in list(self.messages):
                            print(msg)  # imprime cada mensagem restaurada

                    self._log_info(f"CHECKPOINT: Estado restaurado de {self.checkpoint_file} ({num_restored} msgs).")
                    # Loga detalhes adicionais apenas se verbose
                    self._log_verbose(
                        f" -> Seq Num: {self.sequence_number}, Highest Seen: {self.highest_sequence_number}")
                    self._log_verbose(f" -> Peers conhecidos: {self.peers}")
                else:
                    # Arquivo existe mas e de outro ID, informa e ignora
                    self._log_info(
                        f"CHECKPOINT: Arquivo encontrado pertence a outro ID ({state.get('client_id')}). Ignorando.")

            except Exception as e:
                # Erro ao ler ou desserializar o arquivo
                self._log_error(f"Falha ao carregar checkpoint ({self.checkpoint_file}): {e}. Iniciando estado limpo.")
                self.message_log = []  # Garante estado limpo em caso de falha
                self.peers = {self.client_id}
        else:
            # Nenhum arquivo de checkpoint encontrado
            self._log_info("CHECKPOINT: Nenhum arquivo encontrado. Iniciando estado limpo.")

    def _periodic_checkpoint(self):
        # Thread que chama a funçao de salvar checkpoint periodicamente.
        self._log_verbose("Thread de Checkpoint iniciada.")
        while self.running:
            # Espera o intervalo definido
            time.sleep(CHECKPOINT_INTERVAL_S)
            # Verifica se ainda estamos rodando antes de salvar
            # (evita salvar apos o comando 'sair' ser dado mas antes da thread parar)
            if self.running:
                self._save_checkpoint()
        self._log_verbose("Thread de Checkpoint terminada.")

    # --- Loop Principal e Comandos do Usuario ---

    def start(self):
        # Inicia as threads do cliente e o loop principal de interaçao com o usuario.
        self.running = True

        # --- Inicia Threads ---
        self.receive_thread = threading.Thread(target=self._receive_messages, daemon=True)
        self.checkpoint_thread = threading.Thread(target=self._periodic_checkpoint, daemon=True)
        self.reconcile_thread = threading.Thread(target=self._periodic_reconciler, daemon=True)
        # 'daemon=True' significa que essas threads nao impedirao o programa de sair

        self.receive_thread.start()
        self.checkpoint_thread.start()
        self.reconcile_thread.start()

        # Pequena pausa para dar tempo as threads de inicializarem e sockets se ligarem
        time.sleep(0.5)

        # Mensagens de boas-vindas (visiveis para o usuario)
        print(f"--- Usuario {self.client_id} adicionado ao chat em grupo ({MULTICAST_GROUP}:{MULTICAST_PORT}) ---")
        # O prompt inicial detalhado sera exibido por _redisplay_prompt() abaixo

        # --- Loop de Input do Usuario ---
        try:
            while self.running:
                # Exibe o prompt apropriado (detalhado na primeira vez, curto depois)
                self._redisplay_prompt()
                try:
                    # Espera pelo input do usuario
                    user_input = input()
                except EOFError:  # Captura Ctrl+D em terminais Linux/macOS
                    user_input = "sair"  # Trata como o comando sair
                    print("sair")  # Ecoa para clareza, pois input() nao retorna nada com EOF

                # Verifica se o cliente foi parado enquanto esperava input (raro, mas possivel)
                if not self.running: break

                # Processa o input
                cmd = user_input.lower().strip()  # Converte para minusculas e remove espaços extras

                if cmd == "sair":
                    self.stop()  # Inicia o processo de parada
                    break  # Sai do loop de input
                elif cmd == "sync":
                    # Comando para forçar verificaçao de consistencia local
                    self._log_info("Forçando verificaçao de consistencia local...")
                    # A funçao _check_local_consistency loga detalhes apenas se verbose
                    with self.mutex:  # Adquire lock se a funçao de checagem precisar
                        self._check_local_consistency()
                    # Continua para o proximo prompt sem enviar mensagem
                    continue
                elif cmd == "peers":
                    # Comando para listar peers detectados
                    with self.mutex:  # Protege acesso a lista de peers
                        # Mostra peers mesmo em modo nao-verbose, pois e comando explicito
                        print(f"\n[INFO] Usuarios detectados: {self.peers}")
                        self._redisplay_prompt()  # Redesenha o prompt
                    continue
                elif cmd == "verbose":
                    # Comando para alternar o modo verbose
                    self.verbose_mode = not self.verbose_mode  # Inverte o valor da flag
                    status = "ATIVADO" if self.verbose_mode else "DESATIVADO"
                    self._log_info(f"Modo Detalhado (verbose) {status}.")
                    continue  # Continua para o proximo prompt

                # Se nao for um comando conhecido e nao for vazio, trata como mensagem de chat
                elif user_input:
                    self._log_verbose("Tentando enviar mensagem...")
                    # 1. Tenta obter acesso exclusivo usando Ricart-Agrawala
                    if self._request_access():
                        # 2. Se obteve acesso, envia a mensagem
                        self._log_verbose("Permissao OK. Enviando...")
                        msg_content = user_input  # Mantem a capitalizaçao original do usuario
                        message = f"MSG|{self.client_id}|{msg_content}"
                        self._send_multicast(message)

                        # Nota: A mensagem enviada sera recebida pelo proprio cliente
                        # atraves do loop de recebimento multicast e sera processada
                        # por _handle_chat_message, aparecendo na tela e sendo replicada.

                        # 3. Libera o acesso para outros peers
                        self._release_access()
                    else:
                        # Nao obteve acesso (provavelmente devido a timeout)
                        self._log_info("Nao foi possivel obter permissao para enviar agora. Tente novamente.")

                # Se o input foi vazio (usuario apenas pressionou Enter), o loop continua
                # e o prompt sera redesenhado.

        except KeyboardInterrupt:  # Captura Ctrl+C
            print("\nSaindo ...")
            # O bloco finally cuidara da parada limpa
        finally:
            # Garante que a funçao de parada seja chamada, nao importa como o loop terminou
            self.stop()

    def stop(self):
        # Inicia o processo de parada limpa do cliente.
        # Evita chamadas multiplas se stop() ja estiver em execuçao
        if not self.running:
            return

        print(f"\nEncerrando cliente {self.client_id}...")
        self.running = False  # Sinaliza para as threads pararem seus loops

        # Tenta um ultimo checkpoint antes de sair
        self._log_verbose("Salvando checkpoint final...")
        self._save_checkpoint()

        # Fecha os sockets para desbloquear a thread de recebimento (se estiver bloqueada em recvfrom)
        # e liberar os recursos de rede.
        if self.sock_recv:
            self.sock_recv.close()
            self._log_verbose("Socket de recebimento fechado.")
        if self.sock_send:
            self.sock_send.close()
            self._log_verbose("Socket de envio fechado.")

        # Espera um pouco para que as threads daemon percebam a flag 'running' e terminem.
        # Usar join() com timeout e uma boa pratica.
        threads = [self.receive_thread, self.checkpoint_thread, self.reconcile_thread]
        for t in threads:
            if t and t.is_alive():
                t.join(timeout=1.0)  # Espera ate 1 segundo por thread

        print(f"Cliente {self.client_id} encerrado.")


# --- Ponto de Entrada Principal ---
if __name__ == "__main__":
    # Configura o parser de argumentos da linha de comando
    parser = argparse.ArgumentParser(
        description="Cliente de Chat Multicast Distribuido com Ricart-Agrawala, Replicaçao e Checkpoints."
    )
    # Argumento opcional para definir o ID do cliente. Se nao fornecido, gera um UUID curto.
    parser.add_argument(
        "client_id", nargs='?', default=f"user_{str(uuid.uuid4())[:4]}",
        help="ID unico para este cliente (padrao: user_UUIDcurto)"
    )
    args = parser.parse_args()

    # Cria e inicia o cliente
    client = chat_client(args.client_id)
    try:
        client.start()  # Chama o metodo que inicia as threads e o loop de input
    except Exception as main_e:
        # Captura erros inesperados que podem ocorrer durante a inicializaçao ou loop principal
        print(f"\nOcorreu um erro na execuçao principal: {main_e}")
    finally:
        # Garante que, mesmo em caso de erro fatal, tentemos parar o cliente de forma limpa
        # (embora a flag 'running' possa nao ter sido setada para False nesses casos)
        client.stop()
