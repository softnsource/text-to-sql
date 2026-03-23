import { useState, useEffect } from 'react';


function Dashboard({ token, onLoadModel, onConnectNew }) {
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchModels();
  }, []);

  const fetchModels = async () => {
    try {
      const resp = await fetch(`/api/models`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (resp.ok) {
        setModels(await resp.json());
      }
    } catch(e) { console.error(e); }
    setLoading(false);
  };

  const handleModelClick = async (model) => {
    try {
      const resp = await fetch(`/api/models/${model.id}/load`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (resp.ok) {
        const data = await resp.json();
        onLoadModel({
          sessionId: data.session_id,
          dialect: model.dialect,
          dbName: model.db_name
        });
      }
    } catch (e) { alert("Failed to load model. Backend might need restart."); }
  };

  const handleDeleteModel = async (e, modelId) => {
    e.stopPropagation();
    if (!window.confirm("Are you sure you want to completely delete this database model?")) return;
    
    try {
      const resp = await fetch(`/api/models/${modelId}`, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (resp.ok) {
        setModels(models.filter(m => m.id !== modelId));
      } else {
        alert("Failed to delete model.");
      }
    } catch (e) {
      console.error(e);
      alert("Failed to delete model.");
    }
  };

  if (loading) return <div style={{textAlign:'center', marginTop:'10vh'}}>Loading your models...</div>;

  return (
    <div style={{maxWidth: '1000px', margin: '2rem auto', width:'100%'}}>
      <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom: '2rem'}}>
        <div>
          <h2>My Database Models</h2>
          <p className="text-muted">Select an existing database connection to continue chatting, or create a new one.</p>
        </div>
        <button onClick={onConnectNew}>+ Connect New Database</button>
      </div>

      {models.length === 0 ? (
        <div className="glass-panel" style={{textAlign:'center', padding: '4rem 2rem'}}>
           <p style={{color: '#64748b', marginBottom: '1rem'}}>You haven't setup any databases yet.</p>
           <button onClick={onConnectNew} className="secondary">Setup First Connection</button>
        </div>
      ) : (
        <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '1.5rem'}}>
          {models.map(m => (
            <div 
              key={m.id} 
              className="glass-panel model-card animate-fade-in" 
              style={{padding: '1.5rem', cursor:'pointer', transition: 'all 0.2s', position: 'relative'}}
              onClick={() => handleModelClick(m)}
            >
              <button 
                title="Delete Model"
                style={{position: 'absolute', top: '10px', right: '10px', background: 'transparent', padding: '4px', border: 'none', color: '#ef4444', boxShadow: 'none'}}
                onClick={(e) => handleDeleteModel(e, m.id)}
              >
                <svg width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
              </button>
              
              <div style={{fontWeight: 600, fontSize:'1.1rem', marginBottom:'0.5rem', paddingRight: '20px'}}>{m.name}</div>
              <div style={{fontSize:'0.85rem', color: 'var(--text-muted)'}}>
                 <span style={{textTransform:'uppercase', marginRight:'10px', fontWeight:500}}>{m.dialect}</span>
                 {new Date(m.created_at).toLocaleDateString()}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default Dashboard;
