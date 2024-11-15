# Airflow DAG Templates


Bem-vindo ao repositório de **DAG Templates do Apache Airflow**! Este repositório oferece uma coleção de templates prontos para uso, projetados para simplificar e acelerar a criação de workflows em Airflow.

## 🚀 Introdução

O Apache Airflow é uma plataforma de orquestração de workflows que permite a automação e o agendamento de tarefas. Este repositório contém templates de DAGs que podem ser facilmente adaptados às suas necessidades, oferecendo exemplos práticos de uso de diferentes operadores e técnicas.

## 📋 Pré-requisitos

- Certifique-se de que o **Docker** e o **Docker Compose** estejam instalados em sua máquina. Caso não estejam, siga as instruções de instalação no site oficial do [Docker](https://docs.docker.com/get-docker/) e do [Docker Compose](https://docs.docker.com/compose/install/).


## 📁 Estrutura do Repositório

O repositório contém os seguintes arquivos e diretórios na raiz:

- **`README.md`**: Este arquivo, que fornece uma visão geral do repositório e instruções de uso.
- **`docker-compose.yml`**: Um arquivo de configuração para facilitar a execução do Airflow usando Docker.
- **`.gitignore`**: Especifica quais arquivos e pastas devem ser ignorados pelo Git.
- **`DAGs/`**: Uma pasta que contém os templates de DAGs, organizados em arquivos Python prontos para uso.

## 🛠️ Como Usar

Para utilizar este repositório, siga os passos abaixo:

1. **Clone o repositório**:

   Execute o seguinte comando para clonar o repositório em sua máquina local:

   ```bash
   git clone https://github.com/seu_usuario/nome_do_repositorio.git
    ```

2. **Navegue até o repositório**:

   Execute o seguinte comando para ir para pasta do seu repositório:

   ```bash
   cd Airflow
   ```

3. **Execute o DockerCompose**:

   Execute o seguinte comando para executar o docker compose e provisionar o Airflow em sua máquina:

   ```bash
   cd docker-compose up
   ```

## 📜 Documentação dos DAGs

Cada DAG no repositório possui documentação embutida, acessível diretamente na interface do Airflow. Essa documentação inclui:

- **Descrição da DAG**
- **Descrição das Tarefas**
- **Exemplo de uso**

### Exemplos de DAGs

- **DAG: `dag_template_email_notification`**  
  *Template para envio de alertas por email utilizando SMTP.*

- **DAG: `dag_template_exec_bash_with_python`**  
  *Template para execução de BashOperator encapsulado via Python Operator.*

- **DAG: `dag_template_dynamic_task_mapping`**  
  *Template para utilização de tarefas dinâmicas e criação de múltiplas instâncias.*

## 📧 Contato

Para perguntas ou sugestões, entre em contato comigo no [GitHub](https://github.com/j3ffbruce) ou no [LinkedIn](https://www.linkedin.com/in/jefferson-alves-15732513b/).