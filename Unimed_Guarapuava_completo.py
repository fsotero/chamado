from asyncio import tasks
from multiprocessing.sharedctypes import Value
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import requests 
import json 

endpoint_graphql_dev = Variable.get("graphql_endpoint_dev")
endpoint_graphql_prod = Variable.get("graphql_endpoint_prod")
endpoint_slack = Variable.get("slack_endpoint_postmessage")
token_auth_slack = Variable.get("slack_auth_token")
graphQL_token_credentials = json.dumps({
    "query" : "query {\n login (email: \"" + Variable.get("graphql_email_guarapuava") + "\" password: \""+ Variable.get("graphql_password_guarapuava") + "\") {\n token\n}\n}"
   })

def coleta_token_fastmedic():
    data={
    "codMunicipiocliente": 2,
    "cnesPrestador": "0777897",
    "senhaAcesso": "077789",
    "nomMetodo": "SalvarConsultaIntegracao"
    }
    try:
        print("--------------- Chamando endpoint da FastMadic ---------------")
        r = requests.post("https://saude.fastmedic.com.br/api.integracao/api/FcesAcesso/authenticate", data=json.dumps(data), headers={"Content-type" : "application/json"})

    except requests.exceptions.HTTPError as HTTPError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'HTTP erro: {HTTPError}')
        return ["HTTP"]
    
    except requests.exceptions.ConnectionError as ConnectionError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'Erro de Conexão: {ConnectionError}')
        return ["conexão"]
    
    except requests.exceptions.Timeout as Timeout:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Timeout Error: {Timeout}")
        return ["timeout"]
    
    except requests.exceptions.RequestException as RequestException:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Erro de exceção: {RequestException}")
        return ["exceção"]

    else:
        print("--------------- Sucesso ao coletar o Token de acesso ---------------")
        token_fastmedic = "Bearer " + r.json()["token"]
        return token_fastmedic

def valida_coleta_token_fastmedic(ti):
    retorno_solicita_token_fastmedic = ti.xcom_pull(task_ids="Coleta_Token_Fastmedic")
    if  retorno_solicita_token_fastmedic == ["HTTP"] or retorno_solicita_token_fastmedic == ["conexão"] or retorno_solicita_token_fastmedic == ["timeout"] or retorno_solicita_token_fastmedic == ["exceção"]:
        print("--------------- Houve uma falhar ao tentar coletar o Token de acesso ---------------")
        return 'Erro_Coleta_Token_Fastmedic'
    else:
        print("--------------- Sucesso ao coletar o token de acesso, encaminhando para o próximo passo ---------------")
        return 'Coleta_Token_Graphql' 

def erro_coleta_token_fastmedic():
    print("Erro ao coletar Token")

#Funcao para gerar Token de acesso ao GraphQL
def coleta_token_graphql():
    try:
        print("--------------- Chamando endpoint do GraphQL ---------------")
        r = requests.post(endpoint_graphql_prod, data=graphQL_token_credentials, headers={"Content-type" : "application/json"})

    except requests.exceptions.HTTPError as HTTPError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'HTTP erro: {HTTPError}')
        return ["HTTP"]
    
    except requests.exceptions.ConnectionError as ConnectionError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'Erro de Conexão: {ConnectionError}')
        return ["conexão"]
    
    except requests.exceptions.Timeout as Timeout:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Timeout Error: {Timeout}")
        return ["timeout"]
    
    except requests.exceptions.RequestException as RequestException:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Erro de exceção: {RequestException}")
        return ["exceção"]

    else:
        print("--------------- Sucesso ao coletar o Token de acesso ---------------")
        graphQL_token = "Bearer " + r.json()["data"]["login"]["token"]
        return graphQL_token

#Função BranchPythonOperator que faz a validação do token e decide o caminho da DAG
def valida_coleta_token_graphql(ti):
    retorno_coleta_token_graphql = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    if  retorno_coleta_token_graphql == ["HTTP"] or retorno_coleta_token_graphql == ["conexão"] or retorno_coleta_token_graphql == ["timeout"] or retorno_coleta_token_graphql == ["exceção"]:
        print("--------------- Houve uma falhar ao tentar coletar o Token de acesso ---------------")
        return 'Erro_Coleta_Token_Qraphql'
    else:
        print("--------------- Sucesso ao coletar o token de acesso, encaminhando para o próximo passo ---------------")
        return 'Coleta_Data_Paginas_Graphql' 

#Função para enviar os dados de erro ao Slack
def erro_coleta_token_graphql(ti):
    print("Erro ao Coletar Token")

def coleta_data_paginas_graphql(ti):
    graphQL_token = ti.xcom_pull(task_ids="Coleta_Token_Graphql")
    data_hora = datetime.now()
    print(data_hora)
    data_hora_inicial = data_hora - timedelta(hours = 28, minutes = 1)
    data_hora_final = data_hora - timedelta(hours = 3, minutes = 59)
    data_hora_inicial = data_hora_inicial.isoformat()
    data_hora_final = data_hora_final.isoformat()
    ti.xcom_push(key="data_hora_inicial", value=data_hora_inicial)
    ti.xcom_push(key="data_hora_final", value=data_hora_final)
    pagina = 1
    data = {
    "query": "query getHistoryByDateIntegration($entityId: Int, $initialDate: DateTime, $endDate: DateTime,$limit:Int,$page:Int) {\n getHistoryByDateIntegration(entityId: $entityId, initialDate: $initialDate, endDate:$endDate, limit:$limit,page:$page) {\n totalPages history {\n atendimentoId date, evolutionSoap {\n attendanceType soap {\n subjective objective assessment plan \n} cid ciap user email attendantCpf sgOrgaoProfissional numConsRegistroProfissional ufConsProfissional numeroCPFProfissional        nomeProfissionalSolicitante \n} evolutionRecord {\n atendimentoIdHospital motivo observacao usuario email setor attendantCpf serviceStep \n} evolutionNote {\n attendanceType observacao usuario email attendantCpf sgOrgaoProfissional numConsRegistroProfissional ufConsProfissional numeroCPFProfissional        nomeProfissionalSolicitante \n} tags {\n added removed usuario email attendantCpf \n} chat {\n senderId channel whatsappStatus needActiveContact openWindow messagesUnreaded responsibleEmail dateActiveContact dateLastMessage dateFirstMessage dateWhatsappStatus activeContactStatus usuario attendantCpf }\n paciente {\n nome cpf rgh telefone estado bairro ubs healthCare healthCard weight height age \n} telemedicine {\n meetRoomDoctor meetRoomDateStarted tokenDateSent token waiting usuario email attendantCpf queue {\n monitoring {\n canal dataPrimeiroEnvio dataUltimoEnvio dataWhatsappStatus nEnvios status whatsappStatus complaint usuario email attendantCpf \n} \n} evolutionNote {\n crm dataPrescricao emailMedico medico observacao usuarioMedico \n}scheduling {\n dateRequest slotInitialTime slotEndTime user email type nWhatsappTemplatesSent attendantCpf \n} monitoring { canal dataPrimeiroEnvio dataUltimoEnvio dataWhatsappStatus nEnvios status whatsappStatus complaint usuario email attendantCpf \n} \n} \n} \n} \n}",
    "variables": {
            "entityId": 108,
            "limit": 1000,
            "page" : pagina,
            "initialDate" : data_hora_inicial,
            "endDate" : data_hora_final
        }
    }
    
    try:
        print("--------------- Chamando endpoint do GraphQL ---------------")
        r = requests.post(endpoint_graphql_prod, data=json.dumps(data), headers={"Content-type" : "application/json", "Authorization" : graphQL_token})
        

    except requests.exceptions.HTTPError as HTTPError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'HTTP erro: {HTTPError}')
        return ["HTTP"]
    
    except requests.exceptions.ConnectionError as ConnectionError:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print(f'Erro de Conexão: {ConnectionError}')
        return ["conexão"]
    
    except requests.exceptions.Timeout as Timeout:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Timeout Error: {Timeout}")
        return ["timeout"]
    
    except requests.exceptions.RequestException as RequestException:
        print("--------------- Não foi possível obter o Token de acesso ---------------")
        print (f"Erro de exceção: {RequestException}")
        return ["exceção"]

    else:
        total_paginas = r.json()["data"]["getHistoryByDateIntegration"]["totalPages"]
        return total_paginas

def valida_coleta_data_paginas_graphql(ti):
    retorno_coleta_data_paginas_graphql = ti.xcom_pull(task_ids='Coleta_Data_Paginas_Graphql')
    if  retorno_coleta_data_paginas_graphql == ["HTTP"] or retorno_coleta_data_paginas_graphql == ["conexão"] or retorno_coleta_data_paginas_graphql == ["timeout"] or retorno_coleta_data_paginas_graphql == ["exceção"]:
        print("--------------- Houve uma falhar ao tentar coletar o Token de acesso ---------------")
        return 'Erro_Coleta_Data_Paginas_Qraphql'
    else:
        print("--------------- Sucesso ao coletar o token de acesso, encaminhando para o próximo passo ---------------")
        return 'Coleta_Envia_Payload_Graphql'

def erro_coleta_data_paginas_graphql():
    print("Erro ao coletar datapaginas")

def tratacid(cid):
    print(cid)
    cid = cid[0].split(" ")[0]
    cid = cid.replace(".", "").replace(" ", "")
    if len(cid) > 4:
        cid = cid[0:4]
    return cid

def transforma_payload(payload):
    if payload["evolutionSoap"] != None:
        motivoAtendimento = f'{payload["evolutionSoap"]["soap"]["subjective"]} {payload["evolutionSoap"]["soap"]["objective"]} {payload["evolutionSoap"]["soap"]["assessment"]} {payload["evolutionSoap"]["soap"]["plan"]}'
    else:
        motivoAtendimento = payload["evolutionNote"]["observacao"]
    
    if payload["evolutionSoap"] != None and (len(payload["evolutionSoap"]["cid"]) > 0 or len(payload["evolutionSoap"]["ciap"]) > 0):
        cidAtendimento = tratacid(payload["evolutionSoap"]["cid"]) if payload["evolutionSoap"]["cid"] != None else tratacid(payload["evolutionSoap"]["ciap"])
    else:
        cidAtendimento = "Z000"
    
    if payload["evolutionSoap"] != None:
        if payload["evolutionSoap"]["attendantCpf"] != None:
            numero_cpf_profissional = payload["evolutionSoap"]["attendantCpf"].zfill(11)
        else:
            numero_cpf_profissional = None
    else:
        if payload["evolutionNote"]["attendantCpf"] != None:
            numero_cpf_profissional = payload["evolutionNote"]["attendantCpf"].zfill(11)
        else:
            numero_cpf_profissional = None     
    if payload["evolutionSoap"] != None:
        if payload["evolutionSoap"]["sgOrgaoProfissional"] != None:
            sg_conselho_profissional = payload["evolutionSoap"]["sgOrgaoProfissional"]
        else:
            sg_conselho_profissional = None
    else:
        if payload["evolutionNote"]["sgOrgaoProfissional"] != None:
            sg_conselho_profissional = payload["evolutionNote"]["sgOrgaoProfissional"]
        else:
            sg_conselho_profissional = None 
    if payload["evolutionSoap"] != None:
        if payload["evolutionSoap"]["nomeProfissionalSolicitante"] != None:
            nome_profissional = payload["evolutionSoap"]["nomeProfissionalSolicitante"]
        else:
            nome_profissional = None
    else:
        if payload["evolutionNote"]["nomeProfissionalSolicitante"] != None:
            nome_profissional = payload["evolutionNote"]["nomeProfissionalSolicitante"]
        else:
            nome_profissional = None 
    if payload["evolutionSoap"] != None:
        if payload["evolutionSoap"]["numConsRegistroProfissional"] != None:
            nr_conselho_profissional = payload["evolutionSoap"]["numConsRegistroProfissional"]
        else:
            nr_conselho_profissional = None
    else:
        if payload["evolutionNote"]["numConsRegistroProfissional"] != None:
            nr_conselho_profissional = payload["evolutionNote"]["numConsRegistroProfissional"]
        else:
            nr_conselho_profissional = None
    if payload["evolutionSoap"] != None:
        if payload["evolutionSoap"]["ufConsProfissional"] != None:
            uf_conselho_profissional = payload["evolutionSoap"]["ufConsProfissional"]
        else:
            uf_conselho_profissional = None
    else:
        if payload["evolutionNote"]["ufConsProfissional"] != None:
            uf_conselho_profissional = payload["evolutionNote"]["ufConsProfissional"]
        else:
            uf_conselho_profissional = None
    
    payload = {
                    "usuario" : {
                        "nomeUsuario" : payload["paciente"]["nome"],
                        "numeroCPF" : payload["atendimentoId"].zfill(11)
                    },
                    "profissional" : {
                        "ufConsProfissional" : uf_conselho_profissional,
                        "nomeProfissionalSolicitante" : nome_profissional,
                        "sgOrgaoProfissional" : sg_conselho_profissional,
                        "numConsRegistroProfissional" : nr_conselho_profissional,
                        "numCboProfissional" : "225125",
                        "numeroCPFProfissional" : numero_cpf_profissional
                    },
                    "datConsulta" : payload["date"],
                    "motivoAtendimento" : motivoAtendimento,
                    "cidAtendimento" : cidAtendimento,
                    "tipoCidAtendimento" : "P",
                    "procedimentoBPA" : "0301010250"
                    }
    return payload

def coleta_envia_payload_graphql(ti):
    graphQL_token = ti.xcom_pull(task_ids="Coleta_Token_Graphql")
    fastmedic_token = ti.xcom_pull(task_ids="Coleta_Token_Fastmedic")
    data_hora_inicial = ti.xcom_pull(key="data_hora_inicial", task_ids="Coleta_Data_Paginas_Graphql")
    data_hora_final = ti.xcom_pull(key="data_hora_final", task_ids="Coleta_Data_Paginas_Graphql")
    total_paginas = ti.xcom_pull("Coleta_Data_Paginas_Graphql")
    paginas = 1
    print(total_paginas)
    while paginas <= total_paginas:
        data = {
        "query": "query getHistoryByDateIntegration($entityId: Int, $initialDate: DateTime, $endDate: DateTime,$limit:Int,$page:Int) {\n getHistoryByDateIntegration(entityId: $entityId, initialDate: $initialDate, endDate:$endDate, limit:$limit,page:$page) {\n totalPages history {\n atendimentoId date, evolutionSoap {\n attendanceType soap {\n subjective objective assessment plan \n} cid ciap user email attendantCpf sgOrgaoProfissional numConsRegistroProfissional ufConsProfissional numeroCPFProfissional        nomeProfissionalSolicitante \n} evolutionRecord {\n atendimentoIdHospital motivo observacao usuario email setor attendantCpf serviceStep \n} evolutionNote {\n attendanceType observacao usuario email attendantCpf sgOrgaoProfissional numConsRegistroProfissional ufConsProfissional numeroCPFProfissional        nomeProfissionalSolicitante \n} tags {\n added removed usuario email attendantCpf \n} chat {\n senderId channel whatsappStatus needActiveContact openWindow messagesUnreaded responsibleEmail dateActiveContact dateLastMessage dateFirstMessage dateWhatsappStatus activeContactStatus usuario attendantCpf }\n paciente {\n nome cpf rgh telefone estado bairro ubs healthCare healthCard weight height age \n} telemedicine {\n meetRoomDoctor meetRoomDateStarted tokenDateSent token waiting usuario email attendantCpf queue {\n monitoring {\n canal dataPrimeiroEnvio dataUltimoEnvio dataWhatsappStatus nEnvios status whatsappStatus complaint usuario email attendantCpf \n} \n} evolutionNote {\n crm dataPrescricao emailMedico medico observacao usuarioMedico \n}scheduling {\n dateRequest slotInitialTime slotEndTime user email type nWhatsappTemplatesSent attendantCpf \n} monitoring { canal dataPrimeiroEnvio dataUltimoEnvio dataWhatsappStatus nEnvios status whatsappStatus complaint usuario email attendantCpf \n} \n} \n} \n} \n}",
        "variables": {
                "entityId": 108,
                "limit": 1000,
                "page" : paginas,
                "initialDate" : data_hora_inicial,
                "endDate" : data_hora_final
            }
        }
    
        print("--------------- Chamando endpoint do GraphQL ---------------")
        r = requests.post(endpoint_graphql_prod, data=json.dumps(data), headers={"Content-type" : "application/json", "Authorization" : graphQL_token})
        payloads = r.json()["data"]["getHistoryByDateIntegration"]["history"]
        print(f"--------------- {paginas}ª pagina ---------------")
        
        payloads = [payload for payload in payloads if payload["evolutionSoap"] != None or (payload["evolutionNote"] != None and payload["evolutionNote"]["observacao"] != "Novo responsável pelo atendimento")]
        print(payloads)
        
        for payload in payloads:
            data = transforma_payload(payload)
            print(data)
            print('json: ',json.dumps(data, encoding='utf8'))
            r = requests.post("https://saude.fastmedic.com.br/api.integracao/api/IntegracaoConsulta/SalvarConsultaIntegracao", data=json.dumps(data, encoding='utf8'), headers={"Content-type" : "application/json", "Authorization" : fastmedic_token})
            print(r.status_code)
            print(r.json())
        paginas = paginas + 1
            
    

with DAG('Unimed_Guarapuava_completo', start_date = datetime(2022,8,15), schedule_interval = '0 4 * * *', catchup = False, dagrun_timeout=timedelta(minutes=15)) as dag:

    coleta_token_fastmedic = PythonOperator(
        task_id = "Coleta_Token_Fastmedic",
        python_callable = coleta_token_fastmedic
    )

    valida_coleta_token_fastmedic = BranchPythonOperator(
        task_id = "Valida_Coleta_Token_Fastmedic",
        python_callable = valida_coleta_token_fastmedic
    )

    erro_coleta_token_fastmedic = PythonOperator(
        task_id = "Erro_Coleta_Token_Fastmedic",
        python_callable = erro_coleta_token_fastmedic
    )

    coleta_token_graphql = PythonOperator(
        task_id = "Coleta_Token_Graphql",
        python_callable = coleta_token_graphql
    )

    valida_coleta_token_graphql = BranchPythonOperator(
        task_id = "Valida_Coleta_Token_Graphql",
        python_callable = valida_coleta_token_graphql
    )

    erro_coleta_token_graphql = PythonOperator(
        task_id = "Erro_Coleta_Token_Graphql",
        python_callable = erro_coleta_token_graphql
    )

    coleta_data_paginas_graphql = PythonOperator(
        task_id = "Coleta_Data_Paginas_Graphql",
        python_callable = coleta_data_paginas_graphql
    )
    
    valida_coleta_data_paginas_graphql = BranchPythonOperator(
        task_id = "Valida_Coleta_Data_Paginas_Graphql",
        python_callable = valida_coleta_data_paginas_graphql
    )

    erro_coleta_data_paginas_graphql = PythonOperator(
        task_id = "Erro_Coleta_Data_Paginas_Graphql",
        python_callable = erro_coleta_data_paginas_graphql
    )

    coleta_envia_payload_graphql = PythonOperator(
        task_id = "Coleta_Envia_Payload_Graphql",
        python_callable = coleta_envia_payload_graphql
    )

    coleta_token_fastmedic >> valida_coleta_token_fastmedic
    valida_coleta_token_fastmedic >> erro_coleta_token_fastmedic
    valida_coleta_token_fastmedic >> coleta_token_graphql >> valida_coleta_token_graphql
    valida_coleta_token_graphql >> erro_coleta_token_graphql
    valida_coleta_token_graphql >> coleta_data_paginas_graphql >> valida_coleta_data_paginas_graphql
    valida_coleta_data_paginas_graphql >> erro_coleta_data_paginas_graphql
    valida_coleta_data_paginas_graphql >> coleta_envia_payload_graphql