# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM maven:3.9-eclipse-temurin-11@sha256:0b976a58feb2019ea82a243908e1df24718818863cfdc4bc06f263f84876bb0d AS deps
WORKDIR /app
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

FROM maven:3.9-eclipse-temurin-11@sha256:0b976a58feb2019ea82a243908e1df24718818863cfdc4bc06f263f84876bb0d AS jar
WORKDIR /app
COPY --from=deps /root/.m2 /root/.m2
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests -Pno-resources package

FROM eclipse-temurin:11-jre@sha256:52c91c17a87050336a2f95ed76c268514aca02b71461398045e0b30a328c6db8 AS runtime
WORKDIR /app
COPY --from=jar /app/target/flink-kafscale-demo.jar app.jar
COPY src/main/resources /app/config
ENV KAFSCALE_PROFILE=cluster
ENV KAFSCALE_CONFIG_DIR=/app/config
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
