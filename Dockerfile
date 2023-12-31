FROM amazoncorretto:11.0.21 as build

WORKDIR /kronotop
COPY . /kronotop
RUN ./mvnw -f /kronotop/pom.xml clean package -DskipTests

FROM gcr.io/distroless/java11-debian11
COPY --from=build /kronotop/kronotop/target/kronotop*.jar /kronotop/kronotop-latest.jar
ENTRYPOINT ["java", "-jar", "/kronotop/kronotop-latest.jar"]
