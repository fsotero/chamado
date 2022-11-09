from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import requests 
import json 

#Variaveis de ambiende configuradas dentro do Airflow
endpoint_salto = Variable.get("salto_endpoint_exclusao_cpf")
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
            "text": f"[AIRFLOW] Ouve uma falha ao tentar coletar o token de acesso - DAG: Salto_Exclui_CPF - Task: Coleta_Token_Graphql - Erro: {erro}"
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
        return 'Envia_Dados_GraphQL' 

#Função para enviar os dados de erro ao Slack
def erro_coleta_cpf_salto(ti):
    erro = ti.xcom_pull(task_ids='coleta_cpf_salto')
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar coletar os CPFs na API da Unimed Salto - DAG: Salto_Exclui_CPF - Task: Coleta_CPF_Salto - Erro: {erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#Função que faz o envio dos dados para o GraphQL
def envia_dados_graphql(ti):
    graphQL_token = ti.xcom_pull(task_ids='Coleta_Token_GraphQL')
    payloads = ti.xcom_pull(task_ids='Coleta_CPF_Salto')
    data = {
        "data" : {
            "cpfs" : payloads
        }
    }
    try:
        print('--------------- Enviando CPFs para a API do GraphQL ---------------')
        r = requests.delete("https://graphql.laura-br.com/api/v1/removeRemoteCpf", data=json.dumps(data), headers={"Content-type" : "application/json", "Authorization" : graphQL_token})

    except requests.exceptions.HTTPError as HTTPError:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------") 
        print(f'HTTP erro: {HTTPError}')
        return ["HTTP"]
    
    except requests.exceptions.ConnectionError as ConnectionError:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------") 
        print(f'Erro de Conexão: {ConnectionError}')
        return ["conexão"]
    
    except requests.exceptions.Timeout as Timeout:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------") 
        print (f"Timeout Error: {Timeout}")
        return ["timeout"]
    
    except requests.exceptions.RequestException as RequestException:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------") 
        print (f"Erro de exceção: {RequestException}")
        return ["exceção"]

    except Exception as erro:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------") 
        print (f"Erro de exceção: {erro}")
        return ["exception"]

    else:
        print("--------------- Sucesso ao enviar os CPFs para o GraphQL ---------------") 
        return r

#Função que valida se os dados foram enviados para o GraphQL e decide o caminho da DAG
def valida_envia_dados_graphql(ti):
    retorno_envio_dados = ti.xcom_pull(task_ids='Envia_Dados_GraphQL')
    if  retorno_envio_dados == ["HTTP"] or retorno_envio_dados == ["conexão"] or retorno_envio_dados == ["timeout"] or retorno_envio_dados == ["exceção"] or retorno_envio_dados == ["exception"]:
        print("--------------- Falha ao enviar os CPFs para o GraphQL ---------------")
        return 'Erro_Envia_Dados_Graphql'
    else:
        print("--------------- Sucesso ao enviar os CPFs ao GraphQL, encaminhando para o proximo passo ---------------")
        return 'Sucesso_Exclui_CPF_Salto' 

#Função para enviar os dados de erro ao Slack
def erro_envia_dados_graphql(ti):
    erro = ti.xcom_pull(task_ids='Envia_Dados_GraphQL')
    data = {
            "channel": "C03B1JPL46N",
            "text": f"[AIRFLOW] Ouve uma falha ao tentar enviar os CPFs para a API do GraphQL - DAG: Salto_Exclui_CPF Task: Envia_Dados_GraphQL - Erro: {erro}"
        }
    print("--------------- Chamando endpoint do Slack ---------------")
    r = requests.post(endpoint_slack, data=json.dumps(data),  headers={"Content-type" : "application/json",  "Authorization" : token_auth_slack})

#Função que finaliza a DAG com sucesso
def sucesso_exclui_cpf_salto():
    print("--------------- Sucesso ao Excluir os CPFs no GraphQL ---------------")  

with DAG('Salto_Exclui_CPF', start_date = datetime(2022,8,15), schedule_interval = '0 4 * * *', catchup = False, dagrun_timeout=timedelta(minutes=15)) as dag:

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

    envia_dados_graphql = PythonOperator(
        task_id = 'Envia_Dados_GraphQL',
        python_callable = envia_dados_graphql
    )

    valida_envia_dados_graphql = BranchPythonOperator(
        task_id = 'Valida_Envia_Dados_Gaphql',
        python_callable = valida_envia_dados_graphql
    )

    erro_envia_dados_graphql = PythonOperator(
        task_id = 'Erro_Envia_Dados_Graphql',
        python_callable = erro_envia_dados_graphql
    )

    sucesso_exclui_cpf_salto = PythonOperator(
        task_id = 'Sucesso_Exclui_CPF_Salto',
        python_callable = sucesso_exclui_cpf_salto
    )
    coleta_token_graphql >> valida_token_graphql
    valida_token_graphql >> erro_coleta_token_graphql
    valida_token_graphql >> coleta_cpf_salto >> valida_cpf_salto 
    valida_cpf_salto >> erro_coleta_cpf_salto
    valida_cpf_salto >> envia_dados_graphql >> valida_envia_dados_graphql >> [erro_envia_dados_graphql, sucesso_exclui_cpf_salto]