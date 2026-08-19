# Segurança

Não abra issues públicas com credenciais ou dados pessoais. Revogue imediatamente qualquer segredo exposto e remova-o do histórico Git com o procedimento aprovado pelo proprietário do repositório.

O projeto usa variáveis de ambiente apenas no desenvolvimento local. Ambientes Fabric usarão OIDC/service principal ou identidade gerenciada, Key Vault, RBAC de menor privilégio e conexões parametrizadas por ambiente.

