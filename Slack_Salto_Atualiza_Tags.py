from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from time import sleep
import requests 
import json 

#Variaveis de ambiende configuradas dentro do Airflow
endpoint_salto = Variable.get("salto_endpoint_atualiza_tags")
username_salto = Variable.get("salto_username")
password_salto= Variable.get("salto_password")
endpoint_graphql_dev = Variable.get("graphql_endpoint_dev")
endpoint_graphql_prod = Variable.get("graphql_endpoint_prod")
endpoint_slack = Variable.get("slack_endpoint_postmessage")
token_auth_slack = Variable.get("slack_auth_token")
graphQL_token_credentials = json.dumps({
    "query" : "query {\n login (email: \"" + Variable.get("graphql_email") + "\" password: \""+ Variable.get("graphql_password") + "\") {\n token\n}\n}"
   })

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
def valida_token_graphql(ti):
    retorno_token_graphql = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    print(retorno_token_graphql)
    if  retorno_token_graphql == ["HTTP"] or retorno_token_graphql == ["conexão"] or retorno_token_graphql == ["timeout"] or retorno_token_graphql == ["exceção"]:
        print("--------------- Houve uma falhar ao tentar coletar o Token de acesso ---------------")
        return 'Erro_Coleta_Token_Qraphql'
    else:
        print("--------------- Sucesso ao coletar o token de acesso, encaminhando para o próximo passo ---------------")
        return 'Coleta_CPF_Salto' 

#Função para enviar os dados de erro ao Slack
def erro_coleta_token_graphql(ti):
    erro = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar coletar o token de acesso - DAG: Salto_Atualiza_Tags - Task: Coleta_Token_Graphql - Erro: {erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#função que faz a coleta dos CPFs na API de Salto
def coleta_cpf_salto():
    try:
        print("--------------- Chamando endpoint de Salto ---------------")
        r = requests.get(endpoint_salto, auth=(username_salto, password_salto))

    except requests.exceptions.HTTPError as HTTPError:
        print("--------------- Não foi possível obter os CPFs ---------------")
        print(f'HTTP erro: {HTTPError}')
        return ["HTTP"]
    
    except requests.exceptions.ConnectionError as ConnectionError:
        print("--------------- Não foi possível obter os CPFs ---------------")
        print(f'Erro de Conexão: {ConnectionError}')
        return ["conexão"]
    
    except requests.exceptions.Timeout as Timeout:
        print("--------------- Não foi possível obter os CPFs ---------------")
        print (f"Timeout Error: {Timeout}")
        return ["timeout"]
    
    except requests.exceptions.RequestException as RequestException:
        print("--------------- Não foi possível obter os CPFs ---------------")
        print (f"Erro de exceção: {RequestException}")
        return ["exceção"]

    else:
        print("--------------- Sucesso ao coletar os CPFs ---------------")
        payloads = r.json()
        return payloads

#Função BranchPythonOperator que faz a validação dos CPFs recebidos e decide o caminho da DAG
def valida_cpf_salto(ti):
    retorno_cpf_salto = ti.xcom_pull(task_ids='coleta_cpf_salto')
    if  retorno_cpf_salto == ["HTTP"] or retorno_cpf_salto == ["conexão"] or retorno_cpf_salto == ["timeout"] or retorno_cpf_salto == ["exceção"]:
        print("--------------- Ouve um erro na tentativa de coleta dos CPFs ---------------")
        return 'Erro_Coleta_CPF_Salto'
    else:
        print("--------------- Sucesso ao coletar os CPFs, encaminhando para o próximo passo ---------------")
        return 'Adiciona_Paciente_Elegivel' 

#Função para enviar os dados de erro ao Slack
def erro_coleta_cpf_salto(ti):
    erro = ti.xcom_pull(task_ids='coleta_cpf_salto')
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar coletar os CPFs na API da Unimed Salto - DAG: Salto_Atualiza_Tags - Task: Coleta_CPF_Salto - Erro: {erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#Função que faz o envio dos pacientes elegiveis para o GraphQL
def adiciona_paciente_elegivel(ti):
    graphQL_token = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    payloads = ti.xcom_pull(task_ids='Coleta_CPF_Salto')
    success_count = 0
    i = 0
    erros=[]
    for payload in payloads:
        i+=1
        print(f"--------------- {i}ª inclusão ---------------")           
        data = {
            "query" : "mutation addEligiblePatient ($entityId: Int!, $name: String!, $email: String, $birthdate: String, $cellphone: String!, $cpf: String!, $module: String!, $user: String!, $tags: [String], $complaint: String, $flow: String, $triage: TriageDataInput) {\n addEligiblePatient (entityId : $entityId, name: $name, email : $email, birthdate : $birthdate, cellphone : $cellphone, cpf : $cpf, module: $module, user : $user, tags : $tags, complaint : $complaint, flow : $flow, triage : $triage)\n}",
            "variables" : {
                "entityId" : 112,
                "name" : payload["nome"],
                "cellphone" : payload["telefone"],
                "cpf": payload["cpf"],
                "module":"generalist",
                "user" : "Sistema",
                "complaint": "general-exams",
                "flow": "Exames Gerais",
                "triage": {
                    "label" : "Elegíveis"
                }
            }
        }
        def envia_paciente_elegivel(data, graphQL_token):
            r = requests.post(endpoint_graphql_prod, data=json.dumps(data), headers={"Content-type" : "application/json", "Authorization" : graphQL_token})
            return r

        r = envia_paciente_elegivel(data, graphQL_token)
        tentativas = 0    
        while ((r.status_code == 200 and r.json()["data"]["addEligiblePatient"] == None) or r.status_code >= 400) and tentativas < 3:
            sleep(3)
            print("--------------- Falha ao tentar enviar CPF, tentando novamente ---------------")
            r = envia_paciente_elegivel(data, graphQL_token)
            tentativas+=1
            print(f"{tentativas}ª Tentativa")
        if (r.status_code == 200 and r.json()["data"]["addEligiblePatient"] == None):
            success_count+=1        
        elif tentativas == 3:    
            erros.append(r)
    print(erros)
    for envio in erros:
        ti.xcom_push(key="codigo_de_erro", value=envio.status_code)
        try:
            ti.xcom_push(key="mensagem_de_erro", value=envio.json()["errors"][0]["message"])
        except Exception:
            ti.xcom_push(key="mensagem_de_erro", value=envio.json())
        return ["erro"]
    print("--------------- Sucesso ao enviar os CPFs para atualização das Tags no GraphQL ---------------")
    print(f"--------------- Foram incluidos {success_count} CPFs ---------------")
    return r
    

#Função que valida se os dados foram enviados para o GraphQL e decide o caminho da DAG
def valida_adiciona_paciente_elegivel(ti):
    retorno_adiciona_paciente = ti.xcom_pull(task_ids='Adiciona_Paciente_Elegivel')
    if retorno_adiciona_paciente == ["erro"]:
        print("--------------- Falha ao enviar os CPFs para atualização das Tags no GraphQL ---------------")
        return 'Erro_Adiciona_Paciente_Elegivel'
    else:
        print("--------------- Sucesso ao enviar os CPFs para atualização das Tags no GraphQL, encaminhando para o proximo passo ---------------")
        return 'Adiciona_Paciente_Mutation' 

#Função para enviar os dados de erro ao Slack
def erro_adiciona_paciente_elegivel(ti):
    codigo_erro = ti.xcom_pull(task_ids='Adiciona_Paciente_Elegivel', key="codigo_de_erro")
    mensagem_erro = ti.xcom_pull(task_ids='Adiciona_Paciente_Elegivel', key="mensagem_de_erro")
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar enviar os CPFs para a atualização das Tags no GraphQL - DAG: Salto_Atualiza_Tags - Task: Adiciona_Paciente_Elegivel - Codigo do Erro: {codigo_erro} - Mensagem do Erro: {mensagem_erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#Função que faz o envio dos pacientes para o mutation no GraphQL
def adiciona_paciente_mutation(ti):
    graphQL_token = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    payloads = ti.xcom_pull(task_ids='Coleta_CPF_Salto')
    success_count = 0
    i = 0
    erros = []
    for payload in payloads:
        i+=1
        print(f"--------------- {i}ª inclusão ---------------")           
        data = {
            "query" : "mutation updateTag ($cpf: String!, $module: String!, $tags: [String]!) {\n updateTags (cpf: $cpf, module: $module, tags: $tags)\n}",
            "variables" : {
                "cpf": payload["cpf"],
                "module": "generalist",
                "tags": payload["tags"]
            }
        }
        def envia_paciente_mutation(data, graphQL_token):
            r = requests.post(endpoint_graphql_prod, data=json.dumps(data), headers={"Content-type" : "application/json", "Authorization" : graphQL_token})
            return r
        r = envia_paciente_mutation(data, graphQL_token)
        tentativas = 0    
        while ((r.status_code == 200 and r.json()["data"]["updateTags"] != "Updated Tags") or r.status_code >= 400) and tentativas < 3:
            sleep(3)
            print("--------------- Falha ao tentar enviar CPF, tentando novamente ---------------")
            r = envia_paciente_mutation(data, graphQL_token)
            tentativas+=1
            print(f"{tentativas}ª Tentativa")
        if (r.status_code == 200 and r.json()["data"]["updateTags"] != "Updated Tags"):
            success_count+=1        
        elif tentativas == 3:    
            erros.append(r)
    print(erros)
    for envio in erros:
        ti.xcom_push(key="codigo_de_erro", value=envio.status_code)
        try:
            ti.xcom_push(key="mensagem_de_erro", value=envio.json()["errors"][0]["message"])
        except Exception:
            ti.xcom_push(key="mensagem_de_erro", value=envio.json())
        return ["erro"]
    print("--------------- Sucesso ao enviar os CPFs para o GraphQL ---------------")
    print(f"--------------- Foram incluidos {success_count} CPFs ---------------")
    return r
    
#Função que valida se os dados foram enviados para o GraphQL e decide o caminho da DAG
def valida_adiciona_paciente_mutation(ti):
    retorno_envio_dados = ti.xcom_pull(task_ids='Adiciona_Paciente_Mutation')
    if retorno_envio_dados == ["erro"]:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------")
        return 'Erro_Adiciona_Paciente_Mutation'
    else:
        print("--------------- Sucesso ao enviar os CPFs ao GraphQL, encaminhando para o proximo passo ---------------")
        return 'Sucesso_Atualiza_Tags_Salto' 

#Função para enviar os dados de erro ao Slack
def erro_adiciona_paciente_mutation(ti):
    codigo_erro = ti.xcom_pull(key="codigo_de_erro", task_ids="Adiciona_Paciente_Mutation")
    mensagem_erro = ti.xcom_pull(key="mensagem_de_erro", task_ids="Adiciona_Paciente_Mutation")
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar enviar os CPFs para a API do GraphQL - DAG: Salto_Atualiza_Tags - Task: Adiciona_Paciente_Mutation - Codigo do Erro: {codigo_erro} - Mensagem do Erro: {mensagem_erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#Função que finaliza a DAG com sucesso
def sucesso_atualiza_tags_salto():
    print("--------------- Sucesso ao incluir os CPFs no GraphQL ---------------")  

with DAG('Salto_Atualiza_Tags', start_date = datetime(2022,8,15), schedule_interval = '0 4 * * *', catchup = False, dagrun_timeout=timedelta(minutes=15)) as dag:

    coleta_token_graphql = PythonOperator(
        task_id = 'Coleta_Token_GraphQL',
        python_callable = coleta_token_graphql
    )

    valida_token_graphql = BranchPythonOperator(
        task_id = 'Valida_Token_GraphQL',
        python_callable = valida_token_graphql
    )

    erro_coleta_token_graphql = PythonOperator(
        task_id = 'Erro_Coleta_Token_Qraphql',
        python_callable = erro_coleta_token_graphql
    )

    coleta_cpf_salto = PythonOperator(
        task_id = 'Coleta_CPF_Salto',
        python_callable = coleta_cpf_salto
    )

    valida_cpf_salto = BranchPythonOperator(
        task_id = 'Valida_CPF_Salto',
        python_callable = valida_cpf_salto
    )

    erro_coleta_cpf_salto = PythonOperator(
        task_id = 'Erro_Coleta_CPF_Salto',
        python_callable = erro_coleta_cpf_salto
    )

    adiciona_paciente_elegivel = PythonOperator(
        task_id = 'Adiciona_Paciente_Elegivel',
        python_callable = adiciona_paciente_elegivel
    )

    valida_adiciona_paciente_elegivel = BranchPythonOperator(
        task_id = 'Valida_Adiciona_Paciente_Elegivel',
        python_callable = valida_adiciona_paciente_elegivel
    )

    erro_adiciona_paciente_elegivel = PythonOperator(
        task_id = 'Erro_Adiciona_Paciente_Elegivel',
        python_callable = erro_adiciona_paciente_elegivel
    )

    adiciona_paciente_mutation = PythonOperator(
        task_id = 'Adiciona_Paciente_Mutation',
        python_callable = adiciona_paciente_mutation
    )

    valida_adiciona_paciente_mutation = BranchPythonOperator(
        task_id = 'Valida_Adiciona_Paciente_Mutation',
        python_callable = valida_adiciona_paciente_mutation
    )

    erro_adiciona_paciente_mutation = PythonOperator(
        task_id = 'Erro_Adiciona_Paciente_Mutation',
        python_callable = erro_adiciona_paciente_mutation
    )

    sucesso_atualiza_tags_salto = PythonOperator(
        task_id = 'Sucesso_Atualiza_Tags_Salto',
        python_callable = sucesso_atualiza_tags_salto
    )
    coleta_token_graphql >> valida_token_graphql
    valida_token_graphql >> erro_coleta_token_graphql
    valida_token_graphql >> coleta_cpf_salto >> valida_cpf_salto 
    valida_cpf_salto >> erro_coleta_cpf_salto
    valida_cpf_salto >> adiciona_paciente_elegivel >> valida_adiciona_paciente_elegivel
    valida_adiciona_paciente_elegivel >> erro_adiciona_paciente_elegivel
    valida_adiciona_paciente_elegivel >> adiciona_paciente_mutation >> valida_adiciona_paciente_mutation >> [erro_adiciona_paciente_mutation, sucesso_atualiza_tags_salto]