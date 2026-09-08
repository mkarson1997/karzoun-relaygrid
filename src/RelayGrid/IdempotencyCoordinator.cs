using System.Collections.Concurrent;

namespace Karzoun.RelayGrid;

internal sealed class IdempotencyCoordinator
{
    private readonly ConcurrentDictionary<string, byte> _completed = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _inflight = new(StringComparer.Ordinal);

    public async ValueTask<IdempotencyLease?> AcquireAsync(string key)
    {
        while (true)
        {
            if (_completed.ContainsKey(key))
            {
                return null;
            }

            var candidate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (_inflight.TryAdd(key, candidate))
            {
                return new IdempotencyLease(this, key, candidate);
            }

            if (!_inflight.TryGetValue(key, out TaskCompletionSource<bool>? current))
            {
                continue;
            }

            bool succeeded = await current.Task.ConfigureAwait(false);
            if (succeeded)
            {
                return null;
            }
        }
    }

    private void Finish(string key, TaskCompletionSource<bool> owner, bool succeeded)
    {
        if (succeeded)
        {
            _completed.TryAdd(key, 0);
        }

        if (_inflight.TryGetValue(key, out TaskCompletionSource<bool>? current) && ReferenceEquals(current, owner))
        {
            _inflight.TryRemove(key, out _);
        }

        owner.TrySetResult(succeeded);
    }

    internal sealed class IdempotencyLease
    {
        private readonly IdempotencyCoordinator _owner;
        private readonly string _key;
        private readonly TaskCompletionSource<bool> _completion;
        private int _finished;

        public IdempotencyLease(
            IdempotencyCoordinator owner,
            string key,
            TaskCompletionSource<bool> completion)
        {
            _owner = owner;
            _key = key;
            _completion = completion;
        }

        public void Complete(bool succeeded)
        {
            if (Interlocked.Exchange(ref _finished, 1) == 0)
            {
                _owner.Finish(_key, _completion, succeeded);
            }
        }
    }
}
