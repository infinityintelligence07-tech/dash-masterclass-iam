# DASH - Gestão de Masterclass | IAM

Dashboard interativo para gestão de Masterclasses, desenvolvido com **Streamlit**.

## Funcionalidades

- Cadastro e gestão de Masterclasses por polo e cidade
- Definição de metas mensais de vendas e inscrições
- Dashboard com indicadores visuais (KPIs, gráficos interativos)
- Auditoria de ações realizadas
- Interface responsiva com design system IAM

## Tecnologias

- **Python 3.10+**
- **Streamlit** — Interface web interativa
- **Pandas / NumPy** — Manipulação de dados
- **Plotly** — Gráficos interativos

## Como rodar localmente

```bash
# 1. Clone o repositório
git clone https://github.com/SEU-USUARIO/dash-masterclass-iam.git
cd dash-masterclass-iam

# 2. Crie um ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Execute a aplicação
streamlit run app.py
```

A aplicação estará disponível em `http://localhost:8501`.

## Deploy na Vercel

Este projeto está configurado para deploy na Vercel usando o framework **Streamlit**.

### Passo a passo:

1. Faça push do projeto para o GitHub
2. Acesse [vercel.com](https://vercel.com) e importe o repositório
3. Nas configurações do projeto na Vercel:
   - **Framework Preset**: Other
   - **Build Command**: `pip install -r requirements.txt`
   - **Output Directory**: deixe vazio
   - **Install Command**: `pip install -r requirements.txt`
4. Adicione a variável de ambiente se necessário

> **Nota**: A Vercel tem suporte limitado a apps Streamlit. Para deploy em produção, recomendamos o **Streamlit Community Cloud** (gratuito) — basta conectar o repositório GitHub em [share.streamlit.io](https://share.streamlit.io).

## Deploy no Streamlit Community Cloud (Recomendado)

1. Faça push do projeto para o GitHub
2. Acesse [share.streamlit.io](https://share.streamlit.io)
3. Clique em **"New app"**
4. Selecione o repositório, branch `main` e arquivo `app.py`
5. Clique em **Deploy**

Pronto! Sua aplicação estará online em poucos minutos.

## Estrutura do Projeto

```
├── app.py                 # Aplicação principal Streamlit
├── requirements.txt       # Dependências Python
├── .gitignore            # Arquivos ignorados pelo Git
├── .streamlit/
│   └── config.toml       # Configurações do Streamlit
└── README.md             # Este arquivo
```

## Licença

Projeto privado — IAM Liberty Marketing.
