# syntax=docker/dockerfile:1

# ---- build -------------------------------------------------------------------------------------------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore in its own layer: it only re-runs when package versions or project files change.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/CdcPipeline.Core/CdcPipeline.Core.csproj src/CdcPipeline.Core/
COPY src/CdcPipeline.Consumer/CdcPipeline.Consumer.csproj src/CdcPipeline.Consumer/
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet restore src/CdcPipeline.Consumer/CdcPipeline.Consumer.csproj

COPY .editorconfig ./
COPY src/ src/
RUN --mount=type=cache,id=nuget,target=/root/.nuget/packages \
    dotnet publish src/CdcPipeline.Consumer/CdcPipeline.Consumer.csproj \
        --configuration Release --no-restore --output /app/publish -p:UseAppHost=false

# ---- runtime -----------------------------------------------------------------------------------------
# Chiseled Ubuntu: no shell, no package manager, and a non-root "app" user (UID 1654) baked in.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime
LABEL org.opencontainers.image.title="cdc-pipeline-consumer" \
      org.opencontainers.image.source="https://github.com/hekimcan206/cdc-pipeline" \
      org.opencontainers.image.licenses="MIT"
WORKDIR /app
COPY --from=build --link /app/publish .
USER $APP_UID
EXPOSE 8080
ENTRYPOINT ["dotnet", "CdcPipeline.Consumer.dll"]
