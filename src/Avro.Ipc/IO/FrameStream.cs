using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Avro.Ipc.IO
{
    public class FrameStream : Stream
    {
        private const int MIN_FRAME_SIZE = 512;
        private const int DEFAULT_FRAME_SIZE = 2048;
        private readonly int _frameSize;
        private long _position;
        private long _length;
        private long _capacity;
        private int _frameIndex;

        private readonly List<MemoryStream> _frames;
        private readonly List<long> _framesLengths;

        public static readonly FrameStream EMPTY = new FrameStream(MIN_FRAME_SIZE);

        public FrameStream(int frameSize = DEFAULT_FRAME_SIZE)
        {
            if (frameSize < MIN_FRAME_SIZE)
                throw new ArgumentException($"Minimum Frame Size is {MIN_FRAME_SIZE}");
            _frameSize = frameSize;
            _position = 0;
            _length = 0;
            _capacity = 0;
            _frameIndex = 0;
            _frames = new List<MemoryStream>();
            _framesLengths = new List<long>();
        }

        public FrameStream(IEnumerable<MemoryStream> buffers, int frameSize = DEFAULT_FRAME_SIZE)
            : this(frameSize)
        {
            var i = 0;
            var limit = buffers.Count() - 1;
            foreach (var buffer in buffers)
                if (i++ < limit && (buffer.Position != buffer.Length || buffer.Position != buffer.Capacity))
                    throw new ArgumentException($"All but the last buffers must have postion and length equal to capacity");
                else
                    AppendFrame(buffer);
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public override long Length => _length;

        public override long Position { get => _position; set => Seek(value, SeekOrigin.Begin); }

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var i = 0;
            while (i < count)
            {
                var value = ReadByte();
                if (value == -1)
                    break;
                buffer[i + offset] = (byte)value;
                i++;
            }
            return i;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var newPosition = 0L;
            switch (origin)
            {
                case SeekOrigin.Current:
                    newPosition = _position + offset;
                    break;
                case SeekOrigin.Begin:
                    newPosition = offset;
                    break;
                case SeekOrigin.End:
                    newPosition = _length - offset;
                    break;
            }

            if (newPosition < 0 || newPosition > _length)
                throw new InvalidOperationException("Position cannot be less than zero or exceed stream length");

            // Update new position.
            _position = newPosition;
            // Update frame index.
            var newFrameIndex = 0;
            while (newFrameIndex < _framesLengths.Count && _framesLengths[newFrameIndex] < _position)
                newFrameIndex++;
            _frameIndex = newFrameIndex;
            // Set current stream position.
            _frames[_frameIndex].Seek(_framesLengths[_frameIndex] - (_framesLengths[_frameIndex] - _position), SeekOrigin.Begin);
            // Set subsequent streams to position zero.
            for (int i = _frameIndex + 1; i < _frames.Count; i++)
                _frames[i].Seek(0, SeekOrigin.Begin);

            return _position;
        }

        public override void SetLength(long value)
        {
            if (value < 0)
                throw new InvalidOperationException("Length must be zero or a positive number");

            // Update length
            _length = value;
            // Ensure no forward stream position.
            if (_position > _length)
                Seek(_length, SeekOrigin.Begin);
            // Set current frame length.
            _frames[_frameIndex].SetLength(_framesLengths[_frameIndex] - (_framesLengths[_frameIndex] - _length));
            // Reset subsequent streams.
            for (int i = _frameIndex + 1; i < _frames.Count; i++)
            {
                _frames[i].Seek(0, SeekOrigin.Begin);
                _frames[i].SetLength(0);
            }
            // Ensure capacity for the new length.
            while (_capacity < _length)
                NewFrame();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            for (var i = 0; i < count; i++)
                WriteByte(buffer[i + offset]);
        }

        public override void WriteByte(byte value)
        {
            if (_position >= _capacity)
                NewFrame();
            _frames[_frameIndex].WriteByte(value);
            IncrementWrite();
        }

        public override int ReadByte()
        {
            if (_position >= _length)
                return -1;
            var value = _frames[_frameIndex].ReadByte();
            IncrementRead();
            return value;
        }

        public override void CopyTo(Stream destination, int bufferSize)
        {
            for (int i = _frameIndex; i < _frames.Count; i++)
                _frames[i].CopyTo(destination);                
        }

        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            for (int i = _frameIndex; i < _frames.Count; i++)
                await _frames[i].CopyToAsync(destination, cancellationToken);
        }

        public IReadOnlyList<MemoryStream> GetBuffers()
        {
            return _frames.AsReadOnly();
        }

        private void IncrementRead()
        {
            _position++;
            if (_position >= _framesLengths[_frameIndex])
            {
                _frameIndex++;
                if (_frameIndex >= _frames.Count)
                    NewFrame();
            }
        }

        private void IncrementWrite()
        {
            IncrementRead();
            if (_length < _position)
                _length = _position;
        }

        private void NewFrame()
        {
            var frame = new MemoryStream(new byte[_frameSize], 0, _frameSize, true, true);
            frame.SetLength(0);
            AppendFrame(frame);
        }

        public void AppendFrame(MemoryStream buffer)
        {
            if (_position != _capacity)
                throw new InvalidOperationException("Stream must be filled to capacity.");
            _frames.Add(buffer);
            _framesLengths.Add(_capacity + buffer.Capacity);
            _length += buffer.Length;
            _capacity += buffer.Capacity;
        }
    }
}
