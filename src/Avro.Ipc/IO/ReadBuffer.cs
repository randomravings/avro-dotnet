using Avro.Ipc.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Avro.Ipc.IO
{
    public class ReadBuffer : Stream
    {
        private readonly IList<byte[]> _segments;
        private readonly IList<long> _accumulatedSegmentLengths;
        private long _length = 0L;
        private long _postion = 0L;
        private int _segmentIndex = 0;

        public ReadBuffer()
        {
            _segments = new List<byte[]>() { new byte[0] };
            _accumulatedSegmentLengths = new List<long>() { 0L };
        }

        public void AddSegment(byte[] segment)
        {
            _segments.Add(segment);
            var segmentLength = MessageFramingUtil.DecodeLength(segment, 0);

            _accumulatedSegmentLengths.Add(_length + segmentLength);
            _segmentIndex = _segments.Count - 1;
            _postion = segmentLength + 4;
            _length = _accumulatedSegmentLengths.Last();
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Position
        {
            get
            {
                return _accumulatedSegmentLengths[_segmentIndex - 1] + _postion - 4L;
            }
            set
            {
                if (value < 0 || value >= _length)
                    throw new IndexOutOfRangeException();

                _segmentIndex = 1;
                while (_accumulatedSegmentLengths[_segmentIndex] < value)
                    _segmentIndex++;
                _postion = _accumulatedSegmentLengths[_segmentIndex] - (_accumulatedSegmentLengths[_segmentIndex] - value) + 4L;
            }
        }
        public override long Length => _length;

        public override int Read(byte[] buffer, int offset, int count)
        {
            var i = 0;
            for (i = 0; i < count; i++)
                buffer[i + offset] = (byte)ReadByte();
            return i;
        }

        public override int ReadByte()
        {
            if (Position >= Length)
                return -1;
            var b = _segments[_segmentIndex][_postion++];
            if (Position == _accumulatedSegmentLengths[_segmentIndex])
            {
                _segmentIndex++;
                _postion = 4;
            }
            return b;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;
                case SeekOrigin.End:
                    Position = Length - offset;
                    break;
                case SeekOrigin.Current:
                    Position = Position + offset;
                    break;
            }
            return Position;
        }

        public override void Flush() => throw new NotImplementedException();
        public override void SetLength(long value) => throw new NotImplementedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotImplementedException();
    }
}
