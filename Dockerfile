FROM node:22-slim

RUN apt-get update \
&& apt-get install -y zip \
&& apt-get install -y uuid-runtime \
&& apt-get install -y g++

# Create app directory
WORKDIR /api

# Install app dependencies
COPY package*.json ./
COPY tsconfig.json ./
COPY tiffextract ./tiffextract
COPY src ./src

# RUN npm install
# If you are building your code for production
RUN npm install

EXPOSE 443

# Don't use npm start because signals are handled weird. To get a graceful stop need to run node server.js directly
# https://medium.com/@becintec/building-graceful-node-applications-in-docker-4d2cd4d5d392

# Compile
RUN npm run build
RUN g++ ./tiffextract/driver.cpp -o dist/assets/tiffextract.out -fopenmp

WORKDIR /api/dist/app
CMD [ "node", "cluster.js" ]