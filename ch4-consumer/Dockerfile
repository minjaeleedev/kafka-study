# 1단계: 빌드 이미지 (선택사항 - 멀티 스테이지 빌드 사용시)
FROM gradle:8.5-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/project
WORKDIR /home/gradle/project
RUN gradle build --no-daemon

# 2단계: 실제 실행 이미지
FROM openjdk:17-jdk-slim
VOLUME /tmp
ARG JAR_FILE=build/libs/*.jar
COPY --from=build /home/gradle/project/${JAR_FILE} app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
