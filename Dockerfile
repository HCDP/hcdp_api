FROM node:22-slim

RUN apt-get update && apt-get install -y \
  zip \
  uuid-runtime \
  g++ \
  curl \
  && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /api

# Install app dependencies
COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY tiffextract ./tiffextract
COPY src ./src

EXPOSE 8080

# Don't use npm start because signals are handled weird. To get a graceful stop need to run node server.js directly
# https://medium.com/@becintec/building-graceful-node-applications-in-docker-4d2cd4d5d392

# Compile
RUN npm run build
RUN g++ ./tiffextract/driver.cpp -o dist/assets/tiffextract.out -fopenmp

WORKDIR /api/dist/app
CMD [ "node", "cluster.js" ]