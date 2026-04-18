const STEPS = ['Company', 'Objective', 'Targets', 'Upload']

export function ProgressSteps({ current }) {
  return (
    <div className="flex items-center gap-2 mb-6">
      {STEPS.map((step, i) => {
        const state = i < current ? 'done' : i === current ? 'active' : 'pending'
        return (
          <div key={step} className="flex items-center gap-2">
            <div className="flex items-center gap-2">
              <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-medium flex-shrink-0
                ${state === 'done' ? 'bg-teal-accent text-white' : ''}
                ${state === 'active' ? 'bg-teal-dark text-white' : ''}
                ${state === 'pending' ? 'bg-gray-200 text-slate' : ''}
              `}>
                {state === 'done' ? '✓' : i + 1}
              </div>
              <span className={`text-xs ${state === 'active' ? 'text-ink font-medium' : 'text-slate'}`}>
                {step}
              </span>
            </div>
            {i < STEPS.length - 1 && (
              <div className="w-8 h-px bg-gray-200 mx-1" />
            )}
          </div>
        )
      })}
    </div>
  )
}
