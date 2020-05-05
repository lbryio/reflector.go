FROM alpine
EXPOSE 8080

RUN mkdir /app
WORKDIR /app
COPY bin/prism-bin ./prism
RUN chmod +x prism

ENTRYPOINT [ "/app/prism" ]
