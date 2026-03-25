import { useState, useEffect } from 'react';


function Dashboard({ token, onLoadModel, onConnectNew }) {
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);

  const [editingModel, setEditingModel] = useState(null);

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
          dbName: model.db_name,
          commonFilterKeys: model.common_filter_keys || [],
          isOwner: data.is_owner
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

  const handleUpdateFilters = (modelId, newKeys) => {
    setModels(models.map(m => m.id === modelId ? { ...m, common_filter_keys: newKeys } : m));
    setEditingModel(null);
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
              <div style={{position: 'absolute', top: '10px', right: '10px', display: 'flex', gap: '5px'}}>
                {m.is_owner && (
                  <>
                    <button 
                      title="Manage Filters"
                      className="secondary btn-sm"
                      style={{padding: '4px', border: 'none', background: 'transparent', color: 'var(--primary-color)'}}
                      onClick={(e) => { e.stopPropagation(); setEditingModel(m); }}
                    >
                      <svg width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" /><path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" /></svg>
                    </button>
                    <button 
                      title="Delete Model"
                      style={{background: 'transparent', padding: '4px', border: 'none', color: '#ef4444', boxShadow: 'none'}}
                      onClick={(e) => handleDeleteModel(e, m.id)}
                    >
                      <svg width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                    </button>
                  </>
                )}
              </div>

              <div style={{fontWeight: 600, fontSize:'1.1rem', marginBottom:'0.2rem', paddingRight: '60px'}}>{m.name}</div>
              <div style={{fontSize:'0.75rem', color: 'var(--text-muted)', marginBottom: '0.8rem'}}>
                Owner: <span style={{color: 'var(--primary-color)', fontWeight: 600}}>{m.is_owner ? 'You' : m.owner_name}</span>
                {!m.is_owner && <span className="badge" style={{marginLeft: '8px', fontSize: '0.65rem', background: '#f59e0b', color: 'white', padding: '1px 6px'}}>Shared</span>}
              </div>
              
              <div style={{fontSize:'0.85rem', color: 'var(--text-muted)'}}>
                 <span style={{textTransform:'uppercase', marginRight:'10px', fontWeight:500}}>{m.dialect}</span>
                 {new Date(m.created_at).toLocaleDateString()}
              </div>
              {m.common_filter_keys && m.common_filter_keys.length > 0 && (
                <div style={{marginTop: '0.8rem', display: 'flex', gap: '5px', flexWrap: 'wrap'}}>
                  {m.common_filter_keys.map(k => (
                    <span key={k} className="badge" style={{fontSize: '0.7rem', padding: '2px 8px', borderRadius: '10px', background: 'rgba(59, 130, 246, 0.1)', color: 'var(--primary-color)', border: '1px solid rgba(59, 130, 246, 0.2)'}}>
                      {k}
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {editingModel && (
        <EditFiltersModal 
          model={editingModel} 
          token={token} 
          onSave={(newKeys) => handleUpdateFilters(editingModel.id, newKeys)} 
          onCancel={() => setEditingModel(null)} 
        />
      )}
    </div>
  );
}

function EditFiltersModal({ model, token, onSave, onCancel }) {
  const [keys, setKeys] = useState(model.common_filter_keys || []);
  const [newKey, setNewKey] = useState('');

  const handleAdd = () => {
    const sanitized = newKey.trim().toLowerCase();
    if (sanitized && !keys.includes(sanitized)) {
      setKeys([...keys, sanitized]);
      setNewKey('');
    }
  };

  const handleRemove = (key) => {
    setKeys(keys.filter(k => k !== key));
  };

  const handleSave = async () => {
    try {
      const resp = await fetch(`/api/models/${model.id}/metadata`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({ common_filter_keys: keys })
      });
      if (resp.ok) {
        onSave(keys);
      } else {
        alert("Failed to update filters.");
      }
    } catch (e) {
      console.error(e);
      alert("Error updating filters.");
    }
  };

  return (
    <div className="modal-overlay" style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(0,0,0,0.7)', backdropFilter: 'blur(10px)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000
    }}>
      <div className="glass-panel" style={{maxWidth: '450px', width: '90%', padding: '2rem'}}>
        <h3 style={{marginTop: 0}}>Manage Mandatory Filters</h3>
        <p className="text-muted" style={{fontSize: '0.9rem', marginBottom: '1.5rem'}}>
          Define which columns (e.g., team_id) must be filtered in every query for this model.
        </p>

        <div style={{marginBottom: '1.5rem'}}>
          <div style={{display: 'flex', gap: '10px', marginBottom: '1rem'}}>
            <input 
              type="text" 
              placeholder="e.g. team_id" 
              value={newKey} 
              onChange={(e) => setNewKey(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
            />
            <button onClick={handleAdd} className="secondary">+ Add</button>
          </div>

          <div style={{display: 'flex', flexWrap: 'wrap', gap: '8px'}}>
            {keys.length === 0 && <span style={{fontSize: '0.85rem', color: '#94a3b8 italic'}}>No mandatory filters defined.</span>}
            {keys.map(k => (
              <span key={k} className="badge" style={{
                padding: '4px 12px', borderRadius: '20px', background: 'var(--primary-color)', color: 'white',
                display: 'flex', alignItems: 'center', gap: '8px', fontSize: '0.85rem'
              }}>
                {k}
                <span onClick={() => handleRemove(k)} style={{cursor: 'pointer', fontWeight: 800}}>×</span>
              </span>
            ))}
          </div>
        </div>

        <div style={{display: 'flex', gap: '10px', marginTop: '2rem'}}>
          <button className="secondary" style={{flex: 1}} onClick={onCancel}>Cancel</button>
          <button style={{flex: 1}} onClick={handleSave}>Save Changes</button>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
