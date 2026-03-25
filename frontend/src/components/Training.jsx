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
  const [phase, setPhase] = useState('extracting'); // extracting, desc_input, filter_keys, final_training
  const [progress, setProgress] = useState(0);
  const [statusText, setStatusText] = useState("Analyzing your database structure...");
  const [stepsDone, setStepsDone] = useState([]);
  const [schemaReport, setSchemaReport] = useState([]);
  const [customDescs, setCustomDescs] = useState({});
  const [randomMsgIndex, setRandomMsgIndex] = useState(0);
  const [filterKeys, setFilterKeys] = useState(['']); // For mandatory filters

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
            onComplete(filterKeys.filter(k => k.trim() !== ''));
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
    setPhase('filter_keys');
    setStepsDone(prev => [...prev, 'user_descriptions']);
  };

  const handleFilterKeysSubmit = async (e) => {
    e?.preventDefault();
    const cleanKeys = filterKeys.filter(k => k.trim() !== '');
    
    try {
      // Save keys to session context in backend
      const resp = await fetch(`/api/session/${sessionInfo.sessionId}/filter-keys`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionInfo.sessionId, keys: cleanKeys })
      });
      if (!resp.ok) throw new Error("Failed to save mandatory filters");

      setPhase('final_training');
      startFinalTraining();
    } catch (err) {
      onError(err.message);
    }
  };

  const startFinalTraining = async () => {
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
                  onComplete(filterKeys.filter(k => k.trim() !== ''));
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

  const addFilterKeyField = () => setFilterKeys([...filterKeys, '']);
  const updateFilterKey = (index, value) => {
    const newKeys = [...filterKeys];
    newKeys[index] = value;
    setFilterKeys(newKeys);
  };
  const removeFilterKey = (index) => {
    if (filterKeys.length > 1) {
      setFilterKeys(filterKeys.filter((_, i) => i !== index));
    } else {
      setFilterKeys(['']);
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
            <button type="submit">Next: Set Unique Keys →</button>
          </div>
        </form>
      </div>
    );
  }

  if (phase === 'filter_keys') {
    return (
      <div className="training-panel glass-panel animate-fade-in" style={{ maxWidth: '600px' }}>
        <div className="header-text">
          <h1>Set Mandatory Filter Keys <span style={{ fontSize: '0.6em', opacity: 0.6 }}>(Unique Keys)</span></h1>
          <p className="text-muted">Specify column names that MUST be filtered in every query (e.g. user_id, org_id).</p>
        </div>

        <form onSubmit={handleFilterKeysSubmit}>
          <div className="filter-keys-container" style={{ marginBottom: '20px' }}>
            {filterKeys.map((key, idx) => (
              <div key={idx} style={{ display: 'flex', gap: '10px', marginBottom: '10px', alignItems: 'center' }}>
                <input
                  type="text"
                  placeholder="Exact Column Name (e.g. user_id)"
                  value={key}
                  onChange={(e) => updateFilterKey(idx, e.target.value)}
                  style={{ flex: 1 }}
                />
                <button 
                  type="button" 
                  className="secondary" 
                  onClick={() => removeFilterKey(idx)}
                  style={{ padding: '8px 12px', minWidth: 'auto' }}
                >
                  ✕
                </button>
              </div>
            ))}
            <button 
              type="button" 
              className="secondary" 
              onClick={addFilterKeyField}
              style={{ width: '100%', border: '1px dashed var(--border-color)', background: 'transparent' }}
            >
              + Add Another Field
            </button>
          </div>
          <div className="desc-actions">
            <button type="button" className="secondary" onClick={() => handleFilterKeysSubmit()}>Skip - No Restrictions</button>
            <button type="submit">Complete Training →</button>
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
