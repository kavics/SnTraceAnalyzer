namespace SnTraceAnalyzer;


internal class NodeSaveTimeLine
{
    internal class Step
    {
        public string Status { get; set; }
        public string MsgPrefix { get; set; }
        public string MsgSuffix { get; set; }
        public Action<TraceEntry, Row>? ModifyRow { get; set; }
    }
    internal class Row
    {
        public long Id;
        public string? Path;
        public double[] Steps;
    }

    private Step[] _steps;
    private List<Row> _rows = new();

    public NodeSaveTimeLine(Step[] steps)
    {
        _steps = steps;
    }

    public void Parse(IEnumerable<TraceEntry> traceEntries)
    {
        var t0 = traceEntries.First().Time;
        foreach (var entry in traceEntries)
            Parse(entry, t0);

        //// Merge not-saved items with saved ones by theirs object id. A row is "not-saved" when its Id == 0.
        //var notSavedSet = _rows.Where(x => x.Id == 0).ToArray();
        //foreach (var notSaved in notSavedSet)
        //{
        //    var row = _rows.FirstOrDefault(x => x.Id != 0 && x.ObjectId == notSaved.ObjectId);
        //    if (row == null)
        //        continue;

        //    // merge and delete
        //    for (int i = 0; i < notSaved.Steps.Length; i++)
        //        if (notSaved.Steps[i] != default)
        //            row.Steps[i] = notSaved.Steps[i];
        //    _rows.Remove(notSaved);
        //}
    }

    private void Parse(TraceEntry entry, DateTime t0)
    {
        for (var i = 0; i < _steps.Length; i++)
        {
            var step = _steps[i];
            if (entry.Message.StartsWith(step.MsgPrefix))
            {
                if (!string.IsNullOrEmpty(step.MsgSuffix) && !entry.Message.TrimEnd().EndsWith(step.MsgSuffix))
                    continue;
                if (string.IsNullOrEmpty(step.Status) || entry.Status == step.Status)
                {
                    var id = entry.ProgramFlowId;
                    var row = EnsureRow(id, _rows);
                    if (step.ModifyRow != null)
                        step.ModifyRow(entry, row);
                    row.Steps[i] = (entry.Time - t0).TotalSeconds;
                    return;
                }
            }
        }
    }

    private Row EnsureRow(long id, List<Row> rows)
    {
        var existing = rows.FirstOrDefault(x => x.Id == id);
        if (existing == default)
        {
            existing = new Row { Id = id, Steps = new double[_steps.Length] };
            _rows.Add(existing);
        }
        return existing;
    }

    public void OrderById()
    {
        _rows = _rows.OrderBy(x => x.Id).ToList();
    }

    public void Write(TextWriter writer)
    {
        foreach (var row in _rows)
        {
            if (row.Path == null)
                continue;

            var length = row.Steps.Length;

            var t0 = Math.Max(row.Steps[0], Math.Max(row.Steps[4], row.Steps[5]));
            for (int i = 1; i < length; i++)
            {
                if (row.Steps[i] == 0.0d)
                    row.Steps[i] = row.Steps[i - 1];
            }

            writer.Write($"{row.Id}-{row.Path}\t{t0}\t{row.Steps[0]}\t");
            for (int i = 1; i < length; i++)
            {
                writer.Write(row.Steps[i] - row.Steps[i - 1]);
                if (i < length - 1)
                    writer.Write("\t");
            }
            
            writer.WriteLine();
        }
    }
}