namespace SnTraceAnalyzer;

internal class LogEntry
{
    public DateTime Timestamp { get; set; }
    public string Source { get; set; }
    public string Message { get; set; }
    public TraceEntry? Trace { get; set; }
    
}