# LeisMunicipais — códigos por tipo de ato

## Página da cidade `/legislacao-municipal/{id}/leis-de-{slug}`
Checkboxes `name="types"`:
- mais-atos-4   = Leis Complementares
- mais-atos-5   = Decretos
- mais-atos-28  = Leis Ordinárias
- mais-atos-35  = Emendas à Lei Orgânica
- mais-atos-67  = Resoluções da Controladoria Geral
- mais-atos-68  = Resoluções da Sec. Mun. de Administração
- mais-atos-87  = Resoluções da Sec. Mun. de Transportes
- mais-atos-356 = Resoluções da Procuradoria

## URL canônica
Padrão: `/a/{uf}/r/{mun}/{tipo-slug}/{ano}/{cod_url}/{num}/{slug-titulo}`
O `{cod_url}` NÃO é o mesmo do checkbox — varia por lei (provável agrupamento interno).
Não tente montar URL canônica direto. Sempre extraia do resultado da busca.

## Município ID
- rio-de-janeiro/rj: 3613
- curitiba/pr:       5520
- florianopolis/sc:  4571
- porto-alegre/rs:   5519
- belo-horizonte/mg: 1530
- salvador/ba:        532
- recife/pe:         2880
- manaus/am:          157
- aracaju/se:        4661
