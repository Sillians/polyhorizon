FROM node:20-alpine AS build

WORKDIR /app

COPY polyhorizon/frontend/package.json polyhorizon/frontend/package-lock.json /app/
RUN npm ci
COPY polyhorizon/frontend /app

ARG VITE_API_BASE_URL
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}
RUN npm run build

FROM nginx:1.25-alpine
COPY docker/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
