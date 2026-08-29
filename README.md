A Telegram bot designed for climate monitoring and disaster prevention (floods and landslides). The system tracks the geolocation registered by users and triggers automated, real-time risk alerts.

Hybrid Monitoring: Cross-references real-time satellite data (Open-Meteo) with physical government rain gauge measurements (INMET API).

Fault Tolerance (Graceful Degradation): If government servers go down, the engine recalculates the risk using solely satellite data, ensuring the population is not left without alerts.

Scalable Architecture (Clustering): Groups requests by city. If 10,000 people from the same city are registered, the system consumes only 1 API quota and sends the alert via Broadcast.

LGPD Compliance: Transparency in the use of geolocation data and a self-deletion option directly in the bot's menu.

Note: The Telegram API token is kept private, and the bot is currently running exclusively in a local environment.
