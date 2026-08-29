Um bot de Telegram projetado para monitoramento climático e prevenção de desastres (enchentes e deslizamentos).
O sistema rastreia a geolocalização cadastrada pelos usuários e dispara alertas automáticos de risco em tempo real.

Monitoramento Híbrido: Cruza dados de satélite em tempo real (Open-Meteo) com medições de pluviômetros físicos governamentais (API do INMET).

Tolerância a Falhas (Graceful Degradation): Se os servidores do governo caírem, o motor recalcula o risco usando exclusivamente os satélites, garantindo que a população não fique sem alertas.

Arquitetura Escalável (Clusterização): Agrupa requisições por cidade. Se 10.000 pessoas da mesma cidade estiverem cadastradas, o sistema consome apenas 1 cota de API e faz o disparo via Broadcast.

Conformidade com a LGPD: Transparência no uso da geolocalização e opção de autoexclusão de dados direto no menu do bot.
