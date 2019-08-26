namespace Avro.Types
{
    public struct AvroDuration
    {
        public AvroDuration(uint months, uint days, uint milliSeconds)
            : this()
        {
            Months = months;
            Days = days;
            MilliSeconds = milliSeconds;
        }
        public uint Months { get; set; }
        public uint Days { get; set; }
        public uint MilliSeconds { get; set; }
    }
}
