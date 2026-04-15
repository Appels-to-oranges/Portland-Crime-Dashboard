FROM node:20-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY dbt/requirements.txt dbt/requirements.txt
RUN python3 -m venv /opt/dbt-venv \
  && /opt/dbt-venv/bin/pip install --no-cache-dir -r dbt/requirements.txt \
  && ln -sf /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

COPY . .

ENV NODE_ENV=production
EXPOSE 3000

CMD ["node", "src/server.js"]
