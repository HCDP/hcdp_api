FROM node:16

RUN apt-get update \
&& apt-get install -y zip \
&& apt-get install -y uuid-runtime

# Create app directory
WORKDIR /api

# Install app dependencies
COPY package*.json ./
COPY tsconfig.json ./
COPY tiffextract ./tiffextract
COPY src ./src
COPY certs/live/**/*.pem ./src/assets/

# RUN npm install
# If you are building your code for production
RUN npm install --only=production

RUN g++ ./tiffextract/driver.cpp -o tiffextract.out -fopenmp

EXPOSE 443

# Don't use npm start because signals are handled weird. To get a graceful stop need to run node server.js directly
# https://medium.com/@becintec/building-graceful-node-applications-in-docker-4d2cd4d5d392

# Compile
RUN npm run build
WORKDIR /api/dist/app
CMD [ "node", "server.js" ]