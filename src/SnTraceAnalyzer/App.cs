using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace SnTraceAnalyzer;

internal enum Mode { GrayLog, Local, SnTrace }
internal enum Resolution { Normal, High }
internal class App
{
    private readonly Mode _mode;
    private readonly string _logPath;
    private readonly string _outPath;
    private readonly DateTime _start;
    private readonly DateTime _end;
    private readonly Resolution _resolution;

    public App(string[] args)
    {
        _mode = Enum.Parse<Mode>(args[0], true);

        _logPath = args[1];

        _outPath = args[2];

        if (args.Length > 3)
        {
            _start = DateTime.Parse(args[3]);
            _end = DateTime.Parse(args[4]);
        }
        else
        {
            _start = DateTime.MinValue;
            _end = DateTime.MaxValue;
        }

        _resolution = args.Length > 5 ? Enum.Parse<Resolution>(args[5], true) : Resolution.Normal;
    }

    public void Run()
    {
        LogEntry[] entries;
        switch (_mode)
        {
            case Mode.GrayLog:
                entries = GetLogEntries(ParseCsvFromGraylog(_logPath)).ToArray();
                break;
            case Mode.Local:
                entries = GetLogEntries(ParseLocalLog(_logPath)).ToArray();
                break;
            case Mode.SnTrace:
                var logPath = _logPath;
                if (Directory.Exists(logPath))
                    logPath = Directory.GetFiles(logPath, "detailedlog*.log").MaxBy(x => x);
                entries = GetLogEntries(ParseDetailedLog(logPath)).ToArray();
                break;
            default:
                throw new NotSupportedException("Unknown mode: " + _mode);
        }

        // Copy trace time to log time
        foreach (var entry in entries)
            if (entry.Trace != null)
                entry.Timestamp = entry.Trace.Time;

        // Sort by generalized time and use the requested time window.
        entries = entries
            .Where(e => e.Timestamp >= _start && e.Timestamp < _end)
            .OrderBy(e => e.Timestamp)
            .ToArray();
        var trace = entries.Where(e => e.Trace != null).ToArray();

        var firstTime = entries.First().Timestamp;

        // Copy operation duration from end-entries to start-entries
        CopyDurationToStartEntries(trace);

        // Get all sources
        var sources = entries.Select(e => e.Source).Distinct().ToArray();

        // Write all log entries
        WriteToFile("AllEntries.log", entries, firstTime, true, true, false);

        // Write all trace entries
        WriteToFile("Trace\\AllTrace.log", trace, firstTime, true, true, false);

        // Write log entries by source without SnTrace
        var log = entries.Where(e => e.Trace == null).ToArray();
        foreach (var source in sources)
            WriteToFile($@"Log\{source}.txt",
                log.Where(l => l.Source == source).ToArray(), firstTime, false, false, false);

        // Write log entries by progrm-flows with SnTrace and write long events
        var longEntriesPath = Path.Combine(_outPath, "long-entries.txt");
        var longEntriesDir = Path.GetDirectoryName(longEntriesPath);
        if (!Directory.Exists(longEntriesDir))
            Directory.CreateDirectory(longEntriesDir);
        using var longEntriesWriter = new StreamWriter(longEntriesPath);

        var pfMin = trace.Min(e => e.Trace.ProgramFlowId);
        var pfMax = trace.Max(e => e.Trace.ProgramFlowId);
        var pfIds = new List<long>();
        foreach (var source in sources)
        {
            for (var pf = pfMin; pf <= pfMax; pf++)
            {
                var flow = trace.Where(e => e.Source == source && e.Trace.ProgramFlowId == pf).ToArray();
                if (flow.Length > 0)
                {
                    pfIds.Add(pf);
                    var longEntries = WriteToFile($@"Trace\{source}\Pf{pf}.txt", flow, firstTime, true, false, true);
                    if (longEntries.Length > 0)
                    {
                        //longEntriesWriter.WriteLine("Pf:" + pf);
                        longEntriesWriter.Write(longEntries);
                    }
                }
            }
        }

        WriteGantt(trace, sources, pfIds);

        WriteNewSaq1(trace);

        WriteNodeSaveTimeLine(trace);

        WriteBlockedThreads(trace);
    }


    //private void WriteNewSaq(LogEntry[] trace)
    //{
    //    var t0 = trace[0].Timestamp;
    //    var items = new Dictionary<int, NewSaqItem>();
    //    foreach (var logEntry in trace)
    //    {
    //        var entry = logEntry.Trace;
    //        if (entry == null)
    //            continue;
    //        if (entry.Category != "Custom")
    //            continue;


    //        if (entry.Message.Contains("App: Business executes A") && entry.Status == "Start")
    //        {
    //            // Start \t App: Business executes A1
    //            EnsureNewSaqItem(items, ParseItemId(entry.Message, "App: Business executes A".Length)).Start = (entry.Time - t0).TotalSeconds;
    //        }
    //        else if (entry.Message.Contains("ActivityQueue: Arrive A"))
    //        {
    //            // ActivityQueue: Arrive A1 
    //            EnsureNewSaqItem(items, ParseItemId(entry.Message, "ActivityQueue: Arrive A".Length)).Arrived = (entry.Time - t0).TotalSeconds;
    //        }
    //        else if (entry.Message.Contains("Activity: ExecuteInternal A") && entry.Status == "Start")
    //        {
    //            // Start \t Activity: ExecuteInternal A1 (delay: 137) 
    //            EnsureNewSaqItem(items, ParseItemId(entry.Message, "Activity: ExecuteInternal A".Length)).Executing = (entry.Time - t0).TotalSeconds;
    //        }
    //        else if (entry.Message.Contains("Activity: ExecuteInternal A") && entry.Status == "End")
    //        {
    //            // End \t Activity: ExecuteInternal A1 (delay: 137)
    //            EnsureNewSaqItem(items, ParseItemId(entry.Message, "Activity: ExecuteInternal A".Length)).Finished = (entry.Time - t0).TotalSeconds;
    //        }
    //        else if (entry.Message.Contains("App: Business executes A") && entry.Status == "End")
    //        {
    //            // End \t App: Business executes A1
    //            EnsureNewSaqItem(items, ParseItemId(entry.Message, "App: Business executes A".Length)).Released = (entry.Time - t0).TotalSeconds;
    //        }
    //    }

    //    var sorted = items.Values.OrderBy(x => x.Id);
    //    var path = Path.Combine(_outPath, "new-saq.txt");
    //    using var writer = new StreamWriter(path);
    //    writer.WriteLine("Id\tStart\tArrived\tWaiting\tExecution\tFinished");
    //    foreach (var item in sorted)
    //    {
    //        writer.WriteLine($"{item.Id}\t" +
    //                         $"{item.Start}\t" +
    //                         $"{item.Arrived - item.Start}\t" +
    //                         $"{item.Executing - item.Arrived}\t" +
    //                         $"{item.Finished - item.Executing}\t" +
    //                         $"{item.Released - item.Finished}");
    //    }
    //}
    private void WriteNewSaq1(LogEntry[] trace)
    {
        var t0 = trace[0].Timestamp;

        var timeLine = new SaqTimeLine(
            new (string state, string msgPrefix, string msgSuffix)[]
            {
                ("Start", "App: Business executes #SA", ""),
                ("", "DataHandler: SaveActivity #SA", "START"),
                ("", "DataHandler: SaveActivity #SA", "END"),
                ("", "SAQ: Arrive #SA", ""),
                ("", "SAQ: Arrive from receiver #SA", ""),
                ("", "SAQ: Arrive from database #SA", ""),
                ("", "SAQT: execution ignored immediately: #SA", ""),
                ("", "SA: Make dependency: #SA", ""),
                ("", "SAQT: moved to executing list: #SA", ""),
                ("", "SAQT: activity attached to another one: #SA", ""),
                ("", "SAQT: activate dependent: #SA", ""),
                ("", "SAQT: start execution: #SA", ""),
                ("Start", "SA: ExecuteInternal #SA", ""),
                ("End", "SA: ExecuteInternal #SA", ""),
                ("", "SAQT: execution finished: #SA", ""),
                ("", "SAQT: execution ignored (attachment): #SA", ""),
                ("End", "App: Business executes #SA", ""),
            });
        timeLine.Parse(trace.Where(t => t.Trace != null).Select(t => t.Trace));
        timeLine.OrderById();

        var path1 = Path.Combine(_outPath, "saq-timeline.txt");
        using var writer1 = new StreamWriter(path1);
        writer1.WriteLine("Id\tT0\tStart\tSAQStart\tSave\tArrived\tFromNet\tFromDb\tFinishedImmed\tWaitForBlocker\t" +
                          "WaitForExec\tWaitForSame\tWaitingForBlocker\tToExecList\tStartingExec\tEXECUTION\tReleasing\t" +
                          "ReleasingAttachment\tReleased");

        var tMax = trace[^2].Timestamp;
        var headTime = (tMax - t0).TotalSeconds / 17;
        writer1.Write("000-xxx\t0");
        for (int i = 0; i < 17; i++)
            writer1.Write($"\t{headTime}");
        writer1.WriteLine();

        timeLine.Write(writer1);
    }
    //private class NewSaqItem
    //{
    //    public string Id;
    //    public double Start;               // Start    App: Business executes #SA99-61
    //    public double SaveStart;           // Start    DataHandler: SaveActivity #SA99-61
    //    public double SaveEnd;             // End      DataHandler: SaveActivity #SA99-61
    //    public double Arrived;             //          SAQ: Arrive #SA99-61
    //    public double ArrivedFromDb;       //          SAQ: Arrive from database #SA{activity.Key}
    //    public double FinishedImmed;       //          SAQT: execution ignored immediately: #SA{activityToExecute.Key}
    //    public double WaitForBlocker;      //          SA: Make dependency: #SA{Key} depends from SA{olderActivity.Key}.
    //    public double WaitForExec;         //          SAQT: moved to executing list: #SA99-158
    //    public double WaitForSame;         //          SAQT: activity attached to another one: #SA99-165 -> SA99-158
    //    public double BlockerReleased;     //          SAQT: activate dependent: #SA{dependentActivity.Key}
    //    public double StartExecution;      //          SAQT: start execution: #SA99-158
    //    public double Executing;           // Start    SA: ExecuteInternal #SA99-158 (delay: 1)
    //    public double Finished;            // End      SA: ExecuteInternal #SA99-158 (delay: 1)
    //    public double Releasing;           //          SAQT: execution finished: #SA99-158
    //    public double ReleasingAttachment; //          SAQT: execution ignored (attachment): #SA99-165
    //    public double Released;            // End      App: Business executes #SA99-82
    //}
    //private NewSaqItem EnsureNewSaqItem(Dictionary<int, NewSaqItem> items, int id)
    //{
    //    if (items.TryGetValue(id, out var item))
    //        return item;
    //    item = new NewSaqItem { Id = id.ToString() };
    //    items.Add(id, item);
    //    return item;
    //}
    //private NewSaqItem EnsureNewSaqItem1(Dictionary<string, NewSaqItem> items, string key)
    //{
    //    if (key[1] == '-')
    //        key = '0' + key;
    //    if (items.TryGetValue(key, out var item))
    //        return item;
    //    item = new NewSaqItem { Id = key };
    //    items.Add(key, item);
    //    return item;
    //}

    private void WriteNodeSaveTimeLine(LogEntry[] trace)
    {
        var t0 = trace[0].Timestamp;
        var steps = new[]
        {
            new NodeSaveTimeLine.Step {Status = "Start", MsgPrefix = "POST https://"},
            new NodeSaveTimeLine.Step {Status = "Start", MsgPrefix = "NODE.SAVE", ModifyRow = (entry, row) => { row.Path = GetPath(entry); }},
            new NodeSaveTimeLine.Step {Status = "Start", MsgPrefix = "SaveNodeData"},
            new NodeSaveTimeLine.Step {Status = "Start", MsgPrefix = "Indexing node"},
            new NodeSaveTimeLine.Step {Status = "End", MsgPrefix = "Indexing node"},
            new NodeSaveTimeLine.Step {Status = "End", MsgPrefix = "SaveNodeData"},
            new NodeSaveTimeLine.Step {Status = "Start", MsgPrefix = "CreateSecurityEntity"},
            new NodeSaveTimeLine.Step {Status = "End", MsgPrefix = "CreateSecurityEntity"},
            new NodeSaveTimeLine.Step {Status = "End", MsgPrefix = "NODE.SAVE"},
            new NodeSaveTimeLine.Step {Status = "End", MsgPrefix = "POST https://"},
        };
        var timeLine = new NodeSaveTimeLine(steps);

        timeLine.Parse(trace.Where(t => t.Trace != null).Select(t => t.Trace));
        timeLine.OrderById();

        var path = Path.Combine(_outPath, "nodesave-timeline.txt");
        using var writer = new StreamWriter(path);
        writer.WriteLine("Pf\tT0\tStart\tSaveStart\tSaveToDb\tIndexing\tIndexingEnd\tSaveEnd\tSA-Start\tSA-End\t" +
                          "SaveEnd2\tWebEnd");

        var tMax = trace[^2].Timestamp;
        var headTime = (tMax - t0).TotalSeconds / steps.Length;
        writer.Write("000-xxx\t0");
        for (int i = 0; i < steps.Length; i++)
            writer.Write($"\t{headTime}");
        writer.WriteLine();

        timeLine.Write(writer);
    }
    private string? GetPath(TraceEntry entry)
    {
        // NODE.SAVE Id: 0, VersionId: 0, Version: V1.0.A, Name: ex3, ParentPath: /Root/Content/SnRM1
        var msg = entry.Message;
        var pName = msg.IndexOf(", Name:") + 7;
        if (pName == -1)
            return null;
        var pPath = msg.IndexOf(", ParentPath:") + 13;
        if (pPath == -1)
            return null;
        var nameLength = msg.IndexOf(",", pName+1) - pName;
        if (nameLength < 0)
            return null;
        var path = $"{msg.Substring(pPath).Trim()}/{msg.Substring(pName, nameLength).Trim()}";
        return path;
    }


    private void WriteBlockedThreads(LogEntry[] trace)
    {
        var t0 = trace[0].Timestamp;
        var items = new Dictionary<string, BlockedThreadItem>();
        foreach (var logEntry in trace)
        {
            var entry = logEntry.Trace;
            if (entry == null)
                continue;
            if (entry.Message.Length < 3)
                continue;

            var prefix = entry.Message.Substring(0, 3);
            if (entry.Message.Contains(" blocks the "))
            {
                // SAQ: SA{0} blocks the T{1}
                var idIndex = entry.Message.IndexOf(" blocks the ", StringComparison.Ordinal) + 13;
                EnsureBlockedThreadItem(items, prefix + ParseItemId(entry.Message, idIndex)).Block =
                    (entry.Time - t0).TotalSeconds;
            }
            else if (entry.Message.Contains(" waiting resource released "))
            {
                // SAQ: waiting resource released T{0}.
                EnsureBlockedThreadItem(items, prefix + ParseItemId(entry.Message, 32)).Release =
                    (entry.Time - t0).TotalSeconds;
            }
        }

        var sorted = items.Values.OrderBy(x => x.Id);
        var path = Path.Combine(_outPath, "blocked-threads.txt");
        using var writer = new StreamWriter(path);
        writer.WriteLine("Id\tblock\trelease");
        foreach (var item in sorted)
        {
            writer.WriteLine($"{item.Id}\t{item.Block}\t{item.Release - item.Block}");
        }
    }
    private int ParseItemId(string msg, int index)
    {
        var p = index;
        while (msg.Length > p && char.IsDigit(msg[p]))
            p++;
        var src = msg.Substring(index, p - index);
        return int.Parse(src);
    }
    private class BlockedThreadItem
    {
        public string Id;
        public double Block;
        public double Release;
    }
    private BlockedThreadItem EnsureBlockedThreadItem(Dictionary<string, BlockedThreadItem> items, string id)
    {
        if (items.TryGetValue(id, out var item))
            return item;
        item = new BlockedThreadItem { Id = id };
        items.Add(id, item);
        return item;
    }


    private IEnumerable<LogLine> ParseCsvFromGraylog(string logPath)
    {
        using var reader = new StreamReader(logPath);
        var log = reader.ReadToEnd();
        var p = 0;
        var lastP = 0;
        LogLine currentLogLine = null;
        var quotIndex = -1;
        while (p < log.Length)
        {
            var c = log[p++];
            if (c == '"')
            {
                if (p < log.Length && log[p] == '"')
                {
                    p++;
                    continue;
                }
                switch (++quotIndex)
                {
                    case 0: currentLogLine = new LogLine(); lastP = p; break;
                    case 1: currentLogLine.Timestamp = GetField(log, lastP, p); break;
                    case 2: lastP = p; break;
                    case 3: currentLogLine.Source = GetField(log, lastP, p); break;
                    case 4: lastP = p; break;
                    case 5: currentLogLine.Message = GetField(log, lastP, p); break;
                    case 6: lastP = p; break;
                    case 7:
                        currentLogLine.FullMessage = GetField(log, lastP, p);
                        yield return currentLogLine;
                        quotIndex = -1;
                        break;
                }
            }
        }
    }

    private IEnumerable<LogLine> ParseLocalLog(string logPath)
    {
        using var reader = new StreamReader(logPath);
        string? line;
        int p;
        LogLine currentLogLine = null;
        while (null != (line = reader.ReadLine()))
        {
            string dateStr = null;
            DateTime timestamp;
            bool newLogLine = false;
            if ((p = line.IndexOf('[')) > 0)
            {
                dateStr = line.Substring(0, p - 1);
                newLogLine = DateTime.TryParse(dateStr, out timestamp);
            }

            if (newLogLine)
            {
                if (currentLogLine != null)
                    yield return currentLogLine;

                var msg = line.Substring(p);
                p = line.IndexOf(']', p);
                if (p > 0)
                    msg = line.Substring(p + 2);
                currentLogLine = new LogLine { Timestamp = dateStr, Source = "local", FullMessage = msg };
            }
            else
            {
                currentLogLine.FullMessage += "\n" + line;
            }
        }
    }

    private IEnumerable<LogLine> ParseDetailedLog(string logPath)
    {
        using var reader = new StreamReader(logPath);
        string? line;
        while (null != (line = reader.ReadLine()))
        {
            if (line.StartsWith("----"))
                continue;
            if (line.StartsWith("Block size reaches the risky limit:"))
                continue;
            if (line.StartsWith(">"))
                line = line.Substring(1);
            var fields = line.Split('\t');
            yield return new LogLine {Timestamp = fields[1], Source = "detailedlog", FullMessage = line};
        }
    }

    private IEnumerable<LogEntry> GetLogEntries(IEnumerable<LogLine> lines)
    {
        foreach (var line in lines)
        {
            if (DateTime.TryParse(line.Timestamp, out var timestamp))
            {
                var message = line.FullMessage == "null" ? line.Message : line.FullMessage;
                if (TraceEntry.TryParse(message, out var traceEntry))
                {
                    traceEntry.AppDomain = line.Source;
                    //var d = Math.Abs(traceEntry.Time.Ticks - timestamp.Ticks);
                }

                yield return new LogEntry
                {
                    Timestamp = timestamp,
                    Source = line.Source,
                    Message = message,
                    Trace = traceEntry
                };
            }
        }
    }
    private string GetField(string log, int p0, int p1)
    {
        var s = log.Substring(p0, p1 - p0 - 1);
        s = s.Replace("\"\"", "\"");
        return s;
    }

    private string WriteToFile(string relPath, LogEntry[] entries, DateTime firstTime, bool withTrace, bool withSource, bool collectLongEntries)
    {
        var sb = new StringBuilder();
        using var longEventWriter = new StringWriter(sb);

        var path = Path.Combine(_outPath, relPath);
        var dir = Path.GetDirectoryName(path);
        if (!Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        using StreamWriter writer = new StreamWriter(path);

        if(!withTrace)
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tMessage");
        else if (withSource)
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tdT\tSource\tTraceId\tCategory\tThread\tPf\tOp\tStatus\tDuration\tDuration2\tMessage");
        else
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tdT\tTraceId\tCategory\tThread\tPf\tOp\tStatus\tDuration\tDuration2\tMessage");

        var lineId = 0;
        LogEntry lastEntry = null;
        foreach (var logEntry in entries)
        {
            writer.Write($"{++lineId}\t");
            var dt = lastEntry == null ? TimeSpan.Zero : logEntry.Timestamp - lastEntry.Timestamp;
            WriteLogEntry(writer, firstTime, dt, logEntry, withSource);
            if (collectLongEntries)
                if (dt.TotalSeconds >= 1.0d)
                    WriteLogEntry(longEventWriter, firstTime, dt, lastEntry, true);
            lastEntry = logEntry;
        }

        return collectLongEntries ? sb.ToString() : null;
    }
    private void WriteLogEntry(TextWriter writer, DateTime firstTime, TimeSpan dT, LogEntry logEntry, bool withSource)
    {
        var source = withSource ? "\t" + logEntry.Source : string.Empty;

        writer.Write(logEntry.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffff"));
        writer.Write('\t');
        writer.Write((logEntry.Timestamp - firstTime).TotalSeconds.ToString("0.#####", CultureInfo.CurrentCulture));
        writer.Write('\t');
        if (logEntry.Trace == null)
        {
            writer.Write(logEntry.Message);
            if (logEntry.Message.Contains('\n'))
                writer.WriteLine();
        }
        else
        {
            var t = logEntry.Trace;
            var op = t.OpId == 0 ? "" : "Op:" + t.OpId;
            var duration = t.Status == "Start" || t.Status == "End" ? t.Duration.ToString("hh\\:mm\\:ss\\.fffff") : string.Empty;
            var duration2 = t.Status == "Start" || t.Status == "End" ? t.Duration.TotalSeconds.ToString("0.#####", CultureInfo.CurrentCulture) : string.Empty;
            writer.Write($"{dT.ToString("hh\\:mm\\:ss\\.fffff")}{source}\t{t.LineId}\t{t.Category}\tT:{t.ThreadId}\t{t.ProgramFlowId}\t{op}\t{t.Status}\t{duration}\t{duration2}\t{t.Message}");
        }
        writer.WriteLine();
    }

    [SuppressMessage("ReSharper", "ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract")]
    [SuppressMessage("ReSharper", "NullCoalescingConditionIsAlwaysNotNullAccordingToAPIContract")]
    private void WriteGantt(LogEntry[] entries, string[] sources, List<long> pfIds)
    {
        var sourceList = sources.ToList();

        long ticks;
        switch (_resolution)
        {
            case Resolution.Normal: ticks = TimeSpan.TicksPerMillisecond * 100; break;
            case Resolution.High: ticks = TimeSpan.TicksPerMillisecond * 10; break;
            default: throw new ArgumentOutOfRangeException();
        }

        // adjust the start time depending on resolution
        var t0 = entries.Min(e => e.Timestamp);
        var tMin = new DateTime(t0.Year, t0.Month, t0.Day, t0.Hour, t0.Minute, t0.Second);
        double dT;
        switch (_resolution)
        {
            case Resolution.Normal: dT = 0.0; break;
            case Resolution.High: dT = Math.Floor(t0.Millisecond / 100.0) / 10.0; break;
            default: throw new ArgumentOutOfRangeException();
        }
        tMin = tMin.AddSeconds(dT);

        var tMax = entries.Max(e => e.Timestamp);
        var rowCount = Convert.ToInt32((tMax - tMin).Ticks / ticks) + 1;
        var colCountPerSource = pfIds.Count;
        var colCount = colCountPerSource * sources.Length;


        var cells = new List<LogEntry>[rowCount][];
        for (var t = 0; t < rowCount; t++)
            cells[t] = new List<LogEntry>[colCount];

        // Distribute entries by source, Pf and time
        foreach (var entry in entries)
        {
            var t = entry.Timestamp.Ticks - tMin.Ticks;
            var y = t / ticks;
            var sourceIndex = sourceList.IndexOf(entry.Source);
            var x = pfIds.IndexOf(entry.Trace.ProgramFlowId) + sourceIndex * colCountPerSource;
            if (cells[y][x] == null)
                cells[y][x] = new List<LogEntry>();
            cells[y][x].Add(entry);
        }

        // Make gantt columns (inside the gantt line every cell contains a List<LogEntry>)
        for (var x = 0; x < colCount; x++)
        {
            var firstY = -1L;
            for (var y = 0; y < rowCount; y++)
            {
                if (cells[y][x] != null)
                {
                    firstY = y;
                    break;
                }
            }

            var lastY = -1L;
            for (var y = rowCount - 1; y >= 0; y--)
            {
                if (cells[y][x] != null)
                {
                    lastY = y;
                    break;
                }
            }

            // fill inside
            if (firstY >= 0 && lastY < rowCount && firstY <= lastY)
                for (var y = firstY; y <= lastY; y++)
                    cells[y][x] ??= new List<LogEntry>();
        }

        // Find empty columns
        var emptyColumns = new bool[colCount];
        for (var x = 0; x < colCount; x++)
        {
            var isEmpty = true;
            for (var y = 0; y < rowCount; y++)
            {
                if (cells[y][x] != null)
                {
                    isEmpty = false;
                    break;
                }
            }

            emptyColumns[x] = isEmpty;
        }

        // Calculate column counts per source
        var columnCounts = new int[sources.Length];
        for (var i = 0; i < colCount; i += colCountPerSource)
        {
            var sourceIndex = i / colCountPerSource;
            columnCounts[sourceIndex] = colCountPerSource;
            for (var x = i; x < i + colCountPerSource; x++)
                if (emptyColumns[x])
                    columnCounts[sourceIndex]--;
        }

        // Create html
        var path = Path.Combine(_outPath, "gantt-chart.html");
        var dir = Path.GetDirectoryName(path);
        if (!Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        using var writer = new StreamWriter(path);

        writer.WriteLine("<!DOCTYPE html>");
        writer.WriteLine("<html>");
        writer.WriteLine("<head>");
        writer.WriteLine("<style>");
        writer.WriteLine("table { font-family: consolas; font-size: 14px; }");
        writer.WriteLine("tr { margin: 0; padding: 0; height: 8px;}");
        writer.WriteLine("tr.e { background-color: #F8F8F8;} /* even */");
        writer.WriteLine("tr.o { background-color: #F0F0F0;} /* odd */");
        writer.WriteLine("td { margin: 0; padding: 0; min-width: 8px; width: 8px; height: 8px;}");
        writer.WriteLine("td.headCol { min-width: 80px; width: 80px; font-family: consolas; font-size: 12px; vertical-align: top; }\r\n");
        writer.WriteLine("td.e { background-color: lightblue;} /* empty */");
        writer.WriteLine("td.l { background-color: black;} /* log */");
        writer.WriteLine("td.w { background-color: magenta;} /* web */");
        writer.WriteLine("td.x1 { background-color: red;} /* ?? */");
        writer.WriteLine("td.x2 { background-color: #CC0000;} /* ?? */");
        writer.WriteLine("</style>");
        writer.WriteLine("</head>");
        writer.WriteLine("<body>");
        writer.WriteLine("<table>");

        /*
            // https://stackoverflow.com/questions/19155189/javascript-onclick-event-in-all-cells
            window.onload = function(){
                document.getElementById('tbl1').onclick = function(e){
                    var e = e || window.event;
                    var target = e.target || e.srcElement;
                    if(target.tagName.toLowerCase() ==  "td") {
                        alert(target.innerHTML);
                    }
                };
            };
        */

        writer.Write("<tr>");
        for (int i = 0; i < sources.Length; i++)
            writer.Write($"<td colspan=\"{columnCounts[i]}\">{sources[i]}</td>");
        writer.Write("</tr>");

        var lineNumber = 0;
        var currentTime = tMin;
        double incrementTime;
        switch (_resolution)
        {
            case Resolution.Normal: incrementTime = 1.0; break;
            case Resolution.High: incrementTime = 0.1; break;
            default: throw new ArgumentOutOfRangeException();
        }
        foreach (var line in cells)
        {

            var even = ((lineNumber / 10) % 2) == 0;
            writer.Write($"<tr class=\"{(even ? "e" : "o")}\">");
            var x = 0;

            if ((lineNumber % 10) == 0)
            {
                writer.Write($"<td class=\"headCol\" rowspan=\"10\">{currentTime:HH:mm:ss.f}</td>");
                currentTime = currentTime.AddSeconds(incrementTime);
            }

            lineNumber++;

            foreach (var cell in line)
            {
                if (emptyColumns[x++])
                    continue;

                if (cell == null)
                {
                    writer.Write("<td/>");
                }
                else if (cell.Count == 0)
                {
                    writer.Write("<td class=\"e\"/>");
                }
                else
                {
                    var cssClass = GetTdCssClass(cell);
                    
                    writer.Write($"<td class=\"{cssClass}\" title=\"");
                    WriteTooltip(cell, writer);
                    writer.Write("\"/>");
                }
            }
            writer.Write("</tr>");
        }

        writer.WriteLine("</table>");
        writer.WriteLine("</body>");
        writer.WriteLine("</html>");
    }

    private string GetTdCssClass(List<LogEntry> cell)
    {
        var first = cell.First().Trace;
        var last = cell.Last().Trace;
        if (last != null && last.Category == "SecurityQueue" && last.Message.EndsWith("enqueued."))
        {
            return "x1";
        }
        if (first != null && last != null && first != last)
        {
            const string msg1 = "EFCSecurityDataProvider: SaveSecurityActivity.";
            if (/*first.Message.StartsWith(msg1) && first.Status == "End" ||*/
                last.Message.StartsWith(msg1) && last.Status == "Start")
                return "x2";
        }
        return cell.Any(e => e.Trace?.Category == "Web") ? "w" : "l";
    }

    private void WriteTooltip(List<LogEntry> cell, TextWriter writer)
    {
        foreach (var entry in cell)
        {
            var t = entry.Trace;
            if (t == null)
                continue;
            var op = t.OpId == 0 ? string.Empty : "Op:" + t.OpId;
            var duration = t.Status == "End" ? t.Duration.ToString("hh\\:mm\\:ss\\.fffff") : string.Empty;
            writer.WriteLine($"Pf:{entry.Trace.ProgramFlowId} {entry.Timestamp.ToString("HH:mm:ss.fffff")} " +
                             $"{t.LineId} {t.Category} {op} {t.Status} {duration} {t.Message}");

        }
    }

    private void CopyDurationToStartEntries(LogEntry[] entries)
    {
        var endEntries = entries.Where(x => x.Trace.Status == "End").ToArray();
        foreach (var startEntry in entries.Where(x => x.Trace.Status == "Start"))
        {
            var endEntry = endEntries.FirstOrDefault(x => x.Trace.OpId == startEntry.Trace.OpId);
            if (endEntry != null)
                startEntry.Trace.Duration = endEntry.Trace.Duration;
        }
    }
}