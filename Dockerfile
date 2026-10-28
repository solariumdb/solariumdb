# Runs the test suite on Linux, where the io_uring storage environment joins the default one.
# io_uring needs two docker run flags:
#   --security-opt seccomp=unconfined   (Docker's default seccomp profile blocks io_uring)
#   --ulimit memlock=-1                 (each ring counts against RLIMIT_MEMLOCK)
#
#   docker build -t solarium-test .
#   docker run --rm --security-opt seccomp=unconfined --ulimit memlock=-1 solarium-test

FROM mcr.microsoft.com/dotnet/sdk:10.0
WORKDIR /src
COPY global.json Directory.Build.props Solarium.slnx ./
COPY src/ src/
COPY tests/ tests/
COPY benchmarks/ benchmarks/
RUN dotnet restore
RUN dotnet build -c Release --no-restore
ENTRYPOINT ["dotnet", "test", "-c", "Release", "--no-build", "--logger", "console;verbosity=normal"]
