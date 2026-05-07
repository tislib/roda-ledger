import { useState } from 'react';
import { useSubmitOperation } from '@/hooks/useSubmitOperation';
import { allocateUserRef } from '@/lib/ids';

interface Props {
  onTxSubmitted: (txId: string) => void;
}

export function DepositForm({ onTxSubmitted }: Props) {
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
        <FormField label="account" value={account} onChange={setAccount} />
        <FormField label="amount" value={amount} onChange={setAmount} />
      </div>
      <button onClick={onSubmit} disabled={submit.isPending} className="btn w-full justify-center">
        {submit.isPending ? 'Submitting…' : 'Submit deposit'}
      </button>
    </div>
  );
}

interface FieldProps {
  label: string;
  value: string;
  onChange: (v: string) => void;
}

function FormField({ label, value, onChange }: FieldProps) {
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
