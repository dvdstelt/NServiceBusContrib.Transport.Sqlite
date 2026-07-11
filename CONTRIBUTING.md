# Contributing

Contributions are welcome. Please open an issue first for any non-trivial change so the design can be discussed before code is written.

## Development

- .NET 10 SDK
- `dotnet build src/NServiceBusContrib.Transport.Sqlite.slnx` from the repo root
- `dotnet test src/NServiceBusContrib.Transport.Sqlite.slnx` to run all test projects

## Conventions

- Code style is enforced via `.editorconfig` and `EnforceCodeStyleInBuild`. Warnings are treated as errors in Release builds.
- Versioning follows [Semantic Versioning 2.0.0](https://semver.org/); package versions are derived from git tags via MinVer.

## Trademark

This project uses the name `NServiceBus` only in nominative reference (i.e. "for NServiceBus"). Pull requests that put `NServiceBus`, `Particular`, or `NSB` into branding beyond that reference will be declined.
