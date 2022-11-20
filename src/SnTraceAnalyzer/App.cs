using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace SnTraceAnalyzer;

internal enum Mode { GrayLog, Local }
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
        if (_mode == Mode.GrayLog)
            entries = GetLogEntries(ParseCsvFromGraylog(_logPath)).ToArray();
        else if (_mode == Mode.Local)
            entries = GetLogEntries(ParseLocalLog(_logPath)).ToArray();
        else
            throw new NotSupportedException("Unknown mode: " + _mode);

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

        // Copy operation duration from end-entries to start-entries
        CopyDurationToStartEntries(trace);

        // Get all sources
        var sources = entries.Select(e => e.Source).Distinct().ToArray();

        // Write all log entries
        WriteToFile("AllEntries.log", entries, true, true);

        // Write all trace entries
        WriteToFile("Trace\\AllTrace.log", trace, true, true);

        // Write log entries by source without SnTrace
        var log = entries.Where(e => e.Trace == null).ToArray();
        foreach (var source in sources)
            WriteToFile($@"Log\{source}.txt",
                log.Where(l => l.Source == source).ToArray(), false, false);

        // Write log entries by source with SnTrace
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
                    WriteToFile($@"Trace\{source}\Pf{pf}.txt", flow, true, false);
                }
            }
        }

        WriteGantt(trace, sources, pfIds);
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
        return s;
    }

    private void WriteToFile(string relPath, LogEntry[] entries, bool withTrace, bool withSource)
    {
        var path = Path.Combine(_outPath, relPath);
        var dir = Path.GetDirectoryName(path);
        if (!Directory.Exists(dir))
            Directory.CreateDirectory(dir);

        using StreamWriter writer = new StreamWriter(path);

        if(!withTrace)
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tMessage");
        else if (withSource)
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tdT\tSource\tTraceId\tCategory\tPf\tOp\tStatus\tDuration\tDuration2\tMessage");
        else
            writer.WriteLine("LineId\tTimestamp\tTimestamp2\tdT\tTraceId\tCategory\tOp\tStatus\tDuration\tDuration2\tMessage");

        var lineId = 0;
        LogEntry lastEntry = null;
        var firstTime = entries.FirstOrDefault()?.Timestamp ?? DateTime.MinValue;
        foreach (var logEntry in entries)
        {
            writer.Write($"{++lineId}\t");
            var dt = lastEntry == null ? TimeSpan.Zero : logEntry.Timestamp - lastEntry.Timestamp;
            WriteLogEntry(writer, firstTime, dt, logEntry, withSource);
            lastEntry = logEntry;
        }
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
            var pf = withSource ? "\t" + t.ProgramFlowId : string.Empty;
            var op = t.OpId == 0 ? "" : "Op:" + t.OpId;
            var duration = t.Status == "Start" || t.Status == "End" ? t.Duration.ToString("hh\\:mm\\:ss\\.fffff") : string.Empty;
            var duration2 = t.Status == "Start" || t.Status == "End" ? t.Duration.TotalSeconds.ToString("0.#####", CultureInfo.CurrentCulture) : string.Empty;
            writer.Write($"{dT.ToString("hh\\:mm\\:ss\\.fffff")}{source}\t{t.LineId}\t{t.Category}{pf}\t{op}\t{t.Status}\t{duration}\t{duration2}\t{t.Message}");
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