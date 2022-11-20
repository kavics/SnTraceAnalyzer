new SnTraceAnalyzer.App(new[]
{
    "local",
    @"D:\projects\_nlb\15\log-20221117.txt",
    @"D:\projects\_nlb\15\out",
    "2022-11-17 13:16:23.000",
    "2022-11-17 13:16:58.000"
}).Run();
new SnTraceAnalyzer.App(new[]
{
    "local",
    @"D:\projects\_nlb\15\log-20221117.txt",
    @"D:\projects\_nlb\15\out2",
    "2022-11-17 13:16:23.500",
    "2022-11-17 13:16:25.500",
    "high" // normal | high
}).Run();