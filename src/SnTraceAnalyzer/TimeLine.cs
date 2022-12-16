using System;
using System.Globalization;

namespace SnTraceAnalyzer;

internal class TimeLine
{
    private (string Status, string MsgPrefix)[] _steps;
    private List<(string Id, double[] Steps)> _rows = new();

    public TimeLine((string Status, string MsgPrefix)[] steps)
    {
        _steps = steps;
    }

    public void Parse(IEnumerable<TraceEntry> traceEntries)
    {
        var t0 = traceEntries.First().Time;
        foreach (var entry in traceEntries)
            Parse(entry, t0);
    }

    private void Parse(TraceEntry entry, DateTime t0 )
    {
        if (entry.Message.StartsWith("SAQ: Arrive #"))
        {
            int a = 0;
        }
        for (var i = 0; i < _steps.Length; i++)
        {
            var step = _steps[i];
            if (entry.Message.StartsWith(step.MsgPrefix))
            {
                if (string.IsNullOrEmpty(step.Status) || entry.Status == step.Status)
                {
                    var id = ParseId(entry.Message, step.MsgPrefix);
                    if (id == null)
                        continue;
                    var row = EnsureRow(id, _rows);
                    row.steps[i] = (entry.Time - t0).TotalSeconds;
                    return;
                }
            }
        }
    }

    private (string id, double[] steps) EnsureRow(string id, List<(string id, double[] steps)> rows)
    {
        var existing = rows.FirstOrDefault(x => x.id == id);
        if (existing == default)
        {
            existing = (id, new double[_steps.Length]);
            _rows.Add(existing);
        }

        return existing;
    }

    private string ParseId(string msg, string prefix)
    {
        var src = msg.Substring(prefix.Length);
        var p = src.IndexOf(" ");
        if (p > 0)
            src = src.Substring(0, p);
        p = src.IndexOf('-');
        if (p == 1) src = "00" + src;
        if (p == 2) src = "0" + src;
        return src;

    }

    public void OrderById()
    {
        _rows = _rows.OrderBy(x => x.Id).ToList();
    }

    public void Write(TextWriter writer)
    {
        foreach (var row in _rows)
        {
            var length = row.Steps.Length;

            var t0 = Math.Max(row.Steps[0], Math.Max(row.Steps[4], row.Steps[5]));
            for (int i = 1; i < length; i++)
            {
                if (row.Steps[i] == 0.0d)
                    row.Steps[i] = row.Steps[i - 1];
            }

            writer.Write($"{row.Id}\t{t0}\t{row.Steps[0]}\t");
            for (int i = 1; i < length; i++)
            {
                writer.Write(row.Steps[i] - row.Steps[i - 1]);
                if (i < length - 1)
                    writer.Write("\t");
            }


            //writer.Write($"{row.Id}\t");
            //for (int i = 0; i < length; i++)
            //{
            //    writer.Write(row.Steps[i]);
            //    if (i < length - 1)
            //        writer.Write("\t");
            //}
            
            writer.WriteLine();
        }
    }
}