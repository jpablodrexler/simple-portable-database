rmdir /S /Q .\TestResults
rmdir /S /Q .\SimplePortableDatabase.Tests\TestResults

dotnet test /p:CollectCoverage=true /p:CoverletOutput=TestResults/ /p:ExcludeByAttribute="GeneratedCodeAttribute" /p:ExcludeByAttribute="ExcludeFromCodeCoverageAttribute" SimplePortableDatabase.sln --collect:"XPlat Code Coverage"

dotnet tool install dotnet-reportgenerator-globaltool --tool-path Tools
.\Tools\reportgenerator.exe -reports:SimplePortableDatabase.Tests\TestResults\*\coverage.cobertura.xml -targetdir:.\TestResults\

start .\TestResults\index.htm
