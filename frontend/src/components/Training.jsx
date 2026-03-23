import { useState, useEffect } from 'react';


const EXTRACT_MESSAGES = [
  "Please wait, we are fetching the data...",
  "Extracting schema definitions...",
  "Analyzing database structure...",
  "Mapping table relationships...",
  "Preparing metadata..."
];

const INDEX_MESSAGES = [
  "Creating vector store embeddings...",
  "Storing vectors in database...",
  "Building semantic search index...",
  "Please wait for a little while...",
  "Optimizing database context..."
];

function Training({ sessionInfo, onComplete, onError }) {
  const [phase, setPhase] = useState('extracting'); // extracting, desc_input, final_training
  const [progress, setProgress] = useState(0);
  const [statusText, setStatusText] = useState("Analyzing your database structure...");
  const [stepsDone, setStepsDone] = useState([]);
  const [schemaReport, setSchemaReport] = useState([]);
  const [customDescs, setCustomDescs] = useState({});
  const [randomMsgIndex, setRandomMsgIndex] = useState(0);

  useEffect(() => {
    if (phase === 'extracting' || phase === 'final_training') {
      const interval = setInterval(() => {
        setRandomMsgIndex(prev => prev + 1);
      }, 2500);
      return () => clearInterval(interval);
    }
  }, [phase]);

  const currentRandomMsg = phase === 'extracting'
    ? EXTRACT_MESSAGES[randomMsgIndex % EXTRACT_MESSAGES.length]
    : INDEX_MESSAGES[randomMsgIndex % INDEX_MESSAGES.length];

  useEffect(() => {
    if (phase === 'extracting') {
      const source = new EventSource(`/api/db/train/${sessionInfo.sessionId}`);

      source.onmessage = (e) => {
        if (e.data === "keepalive") return;
        try {
          const data = JSON.parse(e.data);
          setProgress(data.progress || 0);
          setStatusText(data.message || "");

          if (data.step && !stepsDone.includes(data.step) && data.step !== 'error') {
            setStepsDone(prev => [...prev, data.step]);
          }

          if (data.step === "tables_ready" || data.step === "schema_extracted") {
            source.close();
            fetchSchemaReport();
          } else if (data.step === "ready") {
            source.close();
            onComplete();
          } else if (data.step === "error") {
            source.close();
            onError(data.error);
          }
        } catch (err) {
          console.error(err);
        }
      };

      source.onerror = (err) => {
        console.error("SSE Error:", err);
      };

      return () => source.close();
    }
  }, [phase, sessionInfo.sessionId]);

  const fetchSchemaReport = async () => {
    try {
      let resp = await fetch(`/api/session/${sessionInfo.sessionId}/schema-report`);
      if (!resp.ok) {
        resp = await fetch(`/api/db/tables/${sessionInfo.sessionId}`);
      }
      const data = await resp.json();
      if (!Array.isArray(data)) throw new Error(data.detail || "Invalid schema data");
      setSchemaReport(data);
      setPhase('desc_input');
    } catch (err) {
      console.error(err);
      onError("Failed to load schema report for descriptions.");
    }
  };

  const handleDescSubmit = async (e) => {
    e?.preventDefault();
    setPhase('final_training');
    setStepsDone(prev => [...prev, 'user_descriptions']);

    try {
      const payload = {
        session_id: sessionInfo.sessionId,
        tables: schemaReport.map(t => ({
          table_name: t.table_name,
          user_description: customDescs[t.table_name] || ""
        }))
      };

      const resp = await fetch(`/api/db/train-with-input`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!resp.ok) {
        const err = await resp.json();
        throw new Error(err.detail || "Build Index Failed");
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        let boundary = buffer.indexOf('\n\n');
        while (boundary !== -1) {
          const chunk = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);

          if (chunk.startsWith('data:')) {
            const dataStr = chunk.replace(/^data:\s*/, '').trim();
            if (dataStr) {
              try {
                const data = JSON.parse(dataStr);
                setProgress(data.progress || 0);
                setStatusText(data.message || "");
                if (data.step && !stepsDone.includes(data.step) && data.step !== 'error') {
                  setStepsDone(prev => [...prev, data.step]);
                }
                if (data.step === "ready") {
                  onComplete();
                  return;
                } else if (data.step === "error") {
                  onError(data.error);
                  return;
                }
              } catch (e) {
                console.error("Failed to parse SSE data", dataStr, e);
              }
            }
          }
          boundary = buffer.indexOf('\n\n');
        }
      }
    } catch (err) {
      onError(err.message);
    }
  };

  if (phase === 'desc_input') {
    return (
      <div className="training-panel glass-panel animate-fade-in" style={{ maxWidth: '800px' }}>
        <div className="header-text">
          <h1>Add Table Descriptions <span style={{ fontSize: '0.6em', opacity: 0.6 }}>(Optional)</span></h1>
          <p className="text-muted">Add business context to help AI understand your data.</p>
        </div>

        <form onSubmit={handleDescSubmit}>
          <div className="desc-container" style={{ maxHeight: '50vh', overflowY: 'auto', paddingRight: '10px' }}>
            {schemaReport.map((t, idx) => (
              <div key={idx} className="desc-table">
                <details>
                  <summary>📋 {t.schema_name ? t.schema_name + '.' : ''}{t.table_name} <span className="text-muted" style={{ fontWeight: 'normal' }}>({t.row_count} rows)</span></summary>
                  <div className="desc-content">
                    <textarea
                      placeholder="e.g. Tracks customer orders with daily sales totals"
                      rows={3}
                      value={customDescs[t.table_name] || ""}
                      onChange={(e) => setCustomDescs({ ...customDescs, [t.table_name]: e.target.value })}
                    ></textarea>
                  </div>
                </details>
              </div>
            ))}
          </div>
          <div className="desc-actions">
            <button type="button" className="secondary" onClick={() => handleDescSubmit()}>Skip - Use AI Only</button>
            <button type="submit">Submit & Index →</button>
          </div>
        </form>
      </div>
    );
  }

  return (
    <div className="training-panel glass-panel animate-fade-in">
      <div className="header-text">
        <h1>🔍 Analyzing Database</h1>
        <p className="text-muted">Reading structure and building semantic search index</p>
      </div>

      <div className="progress-container">
        <div className="progress-bar-bg">
          <div className="progress-bar-fill" style={{ width: `${progress}%` }}></div>
        </div>
        <div className="status-message" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '16px' }}>
          <div className="loader-spinner"></div>
          <div style={{ textAlign: 'left' }}>
            <div>{statusText}</div>
            <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '4px', fontWeight: 'normal' }}>
              {currentRandomMsg}
            </div>
          </div>
        </div>
      </div>

      <div className="steps-list">
        {stepsDone.map((step, i) => (
          <div key={i} className="step-item animate-fade-in">
            <svg width="20" height="20" fill="none" stroke="#10b981" strokeWidth="2" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"></path></svg>
            {step.replace(/_/g, ' ')}
          </div>
        ))}
      </div>
    </div>
  );
}

export default Training;
