import { useState } from 'react';
import { useSubmitOperation } from '@/hooks/useSubmitOperation';
import { useWasmFunctions } from '@/hooks/useWasmFunctions';
import { allocateUserRef } from '@/lib/ids';

interface FormProps {
  onTxSubmitted: (txId: string) => void;
}

function Field({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <div className="flex flex-col gap-0.5">
      <label className="text-[10px] font-mono text-text-muted">{label}</label>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                  focus:outline-none focus:border-accent/60"
        inputMode="numeric"
      />
    </div>
  );
}

export function DepositForm({ onTxSubmitted }: FormProps) {
  const [account, setAccount] = useState('1');
  const [amount, setAmount] = useState('100');
  const submit = useSubmitOperation();
  const onSubmit = async () => {
    const result = await submit.mutateAsync({
      kind: 'Deposit',
      account,
      amount,
      userRef: allocateUserRef(),
    });
    onTxSubmitted(result.txId);
  };
  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Deposit</div>
      <div className="grid grid-cols-2 gap-2">
        <Field label="account" value={account} onChange={setAccount} />
        <Field label="amount" value={amount} onChange={setAmount} />
      </div>
      <button onClick={onSubmit} disabled={submit.isPending} className="btn w-full justify-center">
        {submit.isPending ? 'Submitting…' : 'Submit deposit'}
      </button>
    </div>
  );
}

export function WithdrawalForm({ onTxSubmitted }: FormProps) {
  const [account, setAccount] = useState('1');
  const [amount, setAmount] = useState('50');
  const submit = useSubmitOperation();
  const onSubmit = async () => {
    const result = await submit.mutateAsync({
      kind: 'Withdrawal',
      account,
      amount,
      userRef: allocateUserRef(),
    });
    onTxSubmitted(result.txId);
  };
  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Withdrawal</div>
      <div className="grid grid-cols-2 gap-2">
        <Field label="account" value={account} onChange={setAccount} />
        <Field label="amount" value={amount} onChange={setAmount} />
      </div>
      <button onClick={onSubmit} disabled={submit.isPending} className="btn w-full justify-center">
        {submit.isPending ? 'Submitting…' : 'Submit withdrawal'}
      </button>
    </div>
  );
}

export function TransferForm({ onTxSubmitted }: FormProps) {
  const [from, setFrom] = useState('1');
  const [to, setTo] = useState('2');
  const [amount, setAmount] = useState('100');
  const submit = useSubmitOperation();
  const onSubmit = async () => {
    const result = await submit.mutateAsync({
      kind: 'Transfer',
      from,
      to,
      amount,
      userRef: allocateUserRef(),
    });
    onTxSubmitted(result.txId);
  };
  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Transfer</div>
      <div className="grid grid-cols-3 gap-2">
        <Field label="from" value={from} onChange={setFrom} />
        <Field label="to" value={to} onChange={setTo} />
        <Field label="amount" value={amount} onChange={setAmount} />
      </div>
      <button onClick={onSubmit} disabled={submit.isPending} className="btn w-full justify-center">
        {submit.isPending ? 'Submitting…' : 'Submit transfer'}
      </button>
    </div>
  );
}

export function FunctionInvokeForm({ onTxSubmitted }: FormProps) {
  const { data: deployed } = useWasmFunctions();
  const [name, setName] = useState<string>('');
  const [params, setParams] = useState<string[]>(['0', '0', '0', '0', '0', '0', '0', '0']);
  const submit = useSubmitOperation();

  const selected = deployed?.find((d) => d.name === name) ?? null;

  const onPickFn = (n: string) => {
    setName(n);
    const fn = deployed?.find((d) => d.name === n);
    if (fn) setParams([...fn.defaultParams]);
  };

  const onSubmit = async () => {
    if (!name) return;
    const result = await submit.mutateAsync({
      kind: 'Function',
      name,
      params: params as unknown as readonly [string, string, string, string, string, string, string, string],
      userRef: allocateUserRef(),
    });
    onTxSubmitted(result.txId);
  };

  return (
    <div className="pane p-3 space-y-2">
      <div className="label">Function (WASM)</div>
      {!deployed || deployed.length === 0 ? (
        <div className="text-xs text-text-muted italic">
          No functions deployed. Register one in the Meta module.
        </div>
      ) : (
        <>
          <select
            value={name}
            onChange={(e) => onPickFn(e.target.value)}
            className="w-full bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono"
          >
            <option value="">Select a function…</option>
            {deployed.map((f) => (
              <option key={f.name} value={f.name}>
                {f.name}
              </option>
            ))}
          </select>

          {selected && (
            <>
              <div className="text-[11px] text-text-secondary">{selected.description}</div>
              <div className="grid grid-cols-4 gap-2">
                {params.map((value, idx) => (
                  <div key={idx} className="flex flex-col gap-0.5">
                    <label className="text-[10px] font-mono text-text-muted">p{idx}</label>
                    <input
                      value={value}
                      onChange={(e) => {
                        const v = e.target.value;
                        setParams((prev) => prev.map((p, i) => (i === idx ? v : p)));
                      }}
                      className="bg-bg-2 border border-border rounded px-2 py-1 text-xs font-mono text-text-primary
                                focus:outline-none focus:border-accent/60"
                      inputMode="numeric"
                    />
                  </div>
                ))}
              </div>
            </>
          )}

          <button
            onClick={onSubmit}
            disabled={!name || submit.isPending}
            className="btn btn-accent w-full justify-center"
          >
            {submit.isPending ? 'Invoking…' : 'Invoke function'}
          </button>
        </>
      )}
    </div>
  );
}
