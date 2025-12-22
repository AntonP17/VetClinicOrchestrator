# Этап 1: Сборка приложения
FROM maven:3.8.6-eclipse-temurin-17 as builder
WORKDIR /app

# Копируем только нужные файлы для ускорения сборки
COPY pom.xml .
COPY src ./src

# Собираем JAR с пропуском тестов
RUN mvn clean package -DskipTests

# Этап 2: Запуск приложения (минимальный образ)
FROM eclipse-temurin:17-jre
WORKDIR /app

# Копируем JAR из этапа сборки
COPY --from=builder /app/target/*.jar app.jar

# Указываем точку входа
ENTRYPOINT ["java", "-jar", "app.jar"]