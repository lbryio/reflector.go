FROM alpine
EXPOSE 8080

RUN mkdir /app
WORKDIR /app
COPY dist/linux_amd64/prism-bin ./prism
RUN chmod +x prism

ENTRYPOINT [ "/app/prism" ]
