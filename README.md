## DotNET test project
This is a test project showing how to call most of the functions in the OpenIAP core library.
Program.cs is a command line interface that can be used to call a few functions
test.cs is a simple class that calls all the functions in the core library, you can run this from the cli with the `t` command

# Build and run
```
dotnet run
```
To re-install the openiap package, you can use the following commands
```

rm -rf bin && dotnet nuget locals all --clear && dotnet add package openiap -s ../openiap/dotnet/packages && dotnet run 

rm -rf bin && dotnet nuget locals all --clear && dotnet add package openiap --version 0.0.21 && dotnet run 
```
To clear local cache, in case of any issues
```
dotnet nuget locals all --list
dotnet nuget locals all --clear
```