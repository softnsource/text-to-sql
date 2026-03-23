import { useState, useEffect, useRef } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell
} from 'recharts';


const PaginatedTable = ({ columns, data }) => {
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 5;

  const totalPages = Math.ceil(data.length / rowsPerPage);
  const startIndex = (currentPage - 1) * rowsPerPage;
  const currentData = data.slice(startIndex, startIndex + rowsPerPage);

  const nextPage = () => setCurrentPage(p => Math.min(totalPages, p + 1));
  const prevPage = () => setCurrentPage(p => Math.max(1, p - 1));

  return (
    <>
      <div style={{ overflowX: 'auto' }}>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col, cidx) => <th key={cidx}>{col}</th>)}
            </tr>
          </thead>
          <tbody>
            {currentData.map((row, ridx) => (
              <tr key={ridx}>
                {columns.map((col, cidx) => {
                  const val = row[col] !== null ? String(row[col]) : 'NULL';
                  return <td key={cidx} title={val}>{val}</td>;
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="table-pagination">
          <span>Showing {startIndex + 1} to {Math.min(startIndex + rowsPerPage, data.length)} of {data.length} entries</span>
          <div className="pagination-controls">
            <button type="button" className="pagination-btn" onClick={prevPage} disabled={currentPage === 1}>Prev</button>
            <span className="page-info">Page {currentPage} of {totalPages}</span>
            <button type="button" className="pagination-btn" onClick={nextPage} disabled={currentPage === totalPages}>Next</button>
          </div>
        </div>
      )}
    </>
  );
};

const autodetectChartAxes = (columns, data) => {
  if (columns.length < 2 || data.length === 0) return { xAxis: columns[0] || '', yAxis: columns[1] || '' };

  let xAxis = columns[0];
  let yAxis = columns[1];

  for (const col of columns) {
    const val = data[0][col];
    const numVal = Number(val);
    if (val !== null && val !== "" && !isNaN(numVal) && col.toLowerCase() !== 'id' && !col.toLowerCase().endsWith('id')) {
      yAxis = col;
      break;
    }
  }

  for (const col of columns) {
    if (col === yAxis) continue;
    const val = data[0][col];
    if (typeof val === 'string' && isNaN(Number(val))) {
      xAxis = col;
      break;
    }
  }

  return { xAxis, yAxis };
};

const renderChart = (msg) => {
  if (!msg.tableData || msg.tableData.length === 0 || msg.columns.length === 0) return null;

  const isPie = msg.visualization_hint === 'pie_chart';

  let xAxis = msg.chart_x_axis;
  let yAxis = msg.chart_y_axis;
  let chartData = [];
  let chartTitle = msg.stats?._chart_title || "";

  if (msg.columns.length === 1) {
    xAxis = msg.columns[0];
    yAxis = 'Count';
    const counts = {};
    msg.tableData.forEach(row => {
      const key = String(row[xAxis] || 'Unknown');
      counts[key] = (counts[key] || 0) + 1;
    });
    chartData = Object.keys(counts).map(key => ({
      [xAxis]: key,
      [yAxis]: counts[key]
    }));
    if (!chartTitle) chartTitle = `${yAxis} by ${xAxis}`;
  } else {
    if (!xAxis || !yAxis || xAxis === '' || yAxis === '') {
      const auto = autodetectChartAxes(msg.columns, msg.tableData);
      xAxis = auto.xAxis;
      yAxis = auto.yAxis;
    }

    const hasNumeric = msg.tableData.some(row => {
      const v = row[yAxis];
      return v !== null && v !== "" && !isNaN(Number(v));
    });

    if (!hasNumeric) {
      const counts = {};
      msg.tableData.forEach(row => {
        const key = String(row[xAxis] || 'Unknown');
        counts[key] = (counts[key] || 0) + 1;
      });
      chartData = Object.keys(counts).map(key => ({
        [xAxis]: key,
        [yAxis]: counts[key]
      }));
    } else {
      chartData = msg.tableData.map(row => ({
        ...row,
        [yAxis]: Number(row[yAxis]) || 0,
        [xAxis]: String(row[xAxis])
      }));
    }
    if (!chartTitle) chartTitle = `${yAxis} by ${xAxis}`;
  }

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#14b8a6', '#f97316'];

  return (
    <div className="chart-container" style={{ width: '100%', height: 350, padding: '1rem', backgroundColor: '#ffffff', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
      <h4 style={{ marginBottom: '1rem', textAlign: 'center', color: '#0f172a', fontSize: '0.95rem' }}>{chartTitle}</h4>
      <ResponsiveContainer width="100%" height={280}>
        {isPie ? (
          <PieChart>
            <Tooltip
              contentStyle={{ backgroundColor: '#ffffff', border: '1px solid #e2e8f0', borderRadius: '8px', color: '#0f172a', boxShadow: 'var(--shadow-md)' }}
              itemStyle={{ fontWeight: 'bold' }}
            />
            <Legend verticalAlign="bottom" height={36} />
            <Pie
              data={chartData}
              nameKey={xAxis}
              dataKey={yAxis}
              cx="50%"
              cy="50%"
              outerRadius={90}
              fill="#8884d8"
              label
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
          </PieChart>
        ) : (
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis dataKey={xAxis} stroke="#64748b" tick={{ fill: '#475569', fontSize: 11 }} tickLine={false} axisLine={false} />
            <YAxis stroke="#64748b" tick={{ fill: '#475569', fontSize: 11 }} tickLine={false} axisLine={false} />
            <Tooltip
              cursor={{ fill: '#f1f5f9' }}
              contentStyle={{ backgroundColor: '#ffffff', border: '1px solid #e2e8f0', borderRadius: '8px', color: '#0f172a', boxShadow: 'var(--shadow-md)' }}
              itemStyle={{ color: '#3b82f6', fontWeight: 'bold' }}
            />
            <Bar dataKey={yAxis} fill="#3b82f6" radius={[4, 4, 0, 0]} barSize={40} />
          </BarChart>
        )}
      </ResponsiveContainer>
    </div>
  );
};

const renderDataGrid = (msg, idx) => {
  if (!msg.tableData || !msg.columns || msg.tableData.length === 0) return null;

  const wantsChart = msg.visualization_hint === 'chart' || msg.visualization_hint === 'pie_chart';

  return (
    <div className="data-table-wrapper" style={{ marginTop: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem', padding: '0 8px' }}>
        <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Found {msg.tableData.length} records</span>
      </div>

      {wantsChart ? renderChart(msg) : <PaginatedTable columns={msg.columns} data={msg.tableData} />}
    </div>
  );
};

const renderStats = (msg) => {
  if (!msg.stats) return null;
  const statKeys = Object.keys(msg.stats).filter(k => msg.stats[k].type === 'numeric');
  if (statKeys.length === 0) return null;

  return (
    <div className="stats-grid">
      {statKeys.slice(0, 4).map(k => {
        const s = msg.stats[k];
        return (
          <div key={k} className="stat-chip glass-panel">
            <div className="stat-label">{k}</div>
            <div className="stat-value">{Number(s.avg).toLocaleString()}</div>
          </div>
        );
      })}
    </div>
  );
};

const MessageBubble = ({ msg, idx }) => {
  const [view, setView] = useState('answer');
  const isUser = msg.role === 'user';

  if (isUser) {
    return (
      <div className={`message user animate-fade-in`}>
        <div className="message-bubble">
          {msg.content && msg.content.split('\n').map((line, l) => <p key={l}>{line}</p>)}
        </div>
      </div>
    );
  }

  const renderToggle = () => msg.sql ? (
    <div className="slider-toggle" style={{ float: 'right', marginLeft: '12px', marginBottom: '8px', marginTop: '2px', display: 'flex', zIndex: 10 }}>
      <button
        type="button"
        className={`slider-btn ${view === 'answer' ? 'active' : ''}`}
        onClick={() => setView('answer')}
      >
        Answer
      </button>
      <button
        type="button"
        className={`slider-btn ${view === 'query' ? 'active' : ''}`}
        onClick={() => setView('query')}
      >
        Query
      </button>
      <div className={`slider-bg ${view}`} />
    </div>
  ) : null;

  return (
    <div className={`message assistant animate-fade-in`}>
      <div className="message-bubble" style={{ width: '100%', minWidth: '300px' }}>
        <div>
          {view === 'answer' ? (
            <>
              {renderToggle()}
              <div style={{ minHeight: msg.sql ? '36px' : '0' }}>
                {msg.content && msg.content.split('\n').map((line, l) => <p key={l} style={{ margin: 0, paddingBottom: '8px' }}>{line}</p>)}
              </div>
              <div style={{ clear: 'both' }}></div>
              {renderDataGrid(msg, idx)}
            </>
          ) : (
            <>
              {renderToggle()}
              <div style={{ clear: 'both' }}></div>
              <pre style={{ marginTop: '8px', background: '#1e293b', padding: '12px', borderRadius: '8px', overflowX: 'auto', border: '1px solid #334155' }}>
                <code style={{ color: '#a5b4fc', fontFamily: 'Consolas, monospace' }}>{msg.sql}</code>
              </pre>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

function Chat({ sessionInfo }) {
  const [threads, setThreads] = useState([]);
  const [currentThreadId, setCurrentThreadId] = useState(null);
  const [history, setHistory] = useState([]);
  const [inputData, setInputData] = useState('');
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);

  const endRef = useRef(null);

  useEffect(() => {
    fetchThreads();
  }, [sessionInfo.sessionId]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [history]);

  const fetchThreads = async () => {
    setInitialLoading(true);
    try {
      const token = localStorage.getItem('token');
      const resp = await fetch(`/api/chat/threads`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (resp.ok) {
        setThreads(await resp.json());
      }
    } catch (err) {
      console.error(err);
    } finally {
      setInitialLoading(false);
    }
  };

  const loadThread = async (threadId) => {
    setCurrentThreadId(threadId);
    setLoading(true);
    try {
      const token = localStorage.getItem('token');
      const resp = await fetch(`/api/chat/threads/${threadId}/messages`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (resp.ok) {
        const data = await resp.json();
        setHistory(data.map(m => ({
          role: m.role,
          content: m.content,
          tableData: m.table_data,
          columns: m.columns || [],
          stats: m.stats,
          sql: m.sql_used,
          visualization_hint: m.visualization_hint,
          chart_x_axis: m.chart_x_axis,
          chart_y_axis: m.chart_y_axis
        })));
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const startNewChat = () => {
    setCurrentThreadId(null);
    setHistory([]);
  };

  const handleSend = async (e) => {
    e.preventDefault();
    if (!inputData.trim() || loading) return;

    const question = inputData;
    setInputData('');
    setLoading(true);

    const newHistory = [...history, { role: 'user', content: question }];
    setHistory(newHistory);

    try {
      let activeThreadId = currentThreadId;
      const token = localStorage.getItem('token');
      if (!activeThreadId) {
        const tResp = await fetch(`/api/chat/threads`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
          body: JSON.stringify({ db_session_id: sessionInfo.sessionId, title: question.substring(0, 30) })
        });
        if (tResp.ok) {
          const tData = await tResp.json();
          activeThreadId = tData.id;
          setCurrentThreadId(activeThreadId);
          setThreads(prev => [tData, ...prev]);
        }
      }

      const resp = await fetch(`/api/chat/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionInfo.sessionId, question, page: 1, thread_id: activeThreadId })
      });

      const data = await resp.json();
      if (!resp.ok) throw new Error(data.detail || "Query failed");

      setHistory(prev => [...prev, {
        role: 'assistant',
        content: data.text_summary,
        tableData: data.table_data,
        columns: data.columns,
        stats: data.stats,
        sql: data.sql_used,
        visualization_hint: data.visualization_hint,
        chart_x_axis: data.chart_x_axis,
        chart_y_axis: data.chart_y_axis
      }]);

    } catch (err) {
      setHistory(prev => [...prev, { role: 'assistant', content: `Error: ${err.message}` }]);
    } finally {
      setLoading(false);
    }
  };





  return (
    <div className="chat-layout animate-fade-in">
      {/* Styles for toggle slider and loader */}
      <style>{`
        .slider-toggle {
          background: #f1f5f9;
          border: 1px solid #e2e8f0;
          border-radius: 20px;
          padding: 2px;
          position: relative;
          width: 130px;
          box-shadow: inset 0 1px 3px rgba(0,0,0,0.1);
        }
        .slider-btn {
          flex: 1;
          background: transparent;
          border: none;
          color: #64748b;
          font-size: 0.75rem;
          font-weight: 600;
          padding: 6px 0;
          cursor: pointer;
          position: relative;
          z-index: 2;
          transition: all 0.3s ease;
        }
        .slider-btn:hover {
          color: #3b82f6;
          background: rgba(59, 130, 246, 0.1);
          border-radius: 16px;
        }
        .slider-btn.active {
          color: #ffffff;
        }
        .slider-btn.active:hover {
          color: #ffffff;
          background: transparent;
        }
        .slider-bg {
          position: absolute;
          top: 2px;
          bottom: 2px;
          width: calc(50% - 2px);
          background: #3b82f6;
          border-radius: 18px;
          z-index: 1;
          transition: transform 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
          box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);
        }
        .slider-bg.answer {
          transform: translateX(0%);
        }
        .slider-bg.query {
          transform: translateX(100%);
        }
      `}</style>
      {/* Sidebar */}
      <div className="sidebar glass-panel" style={{ display: 'flex', flexDirection: 'column' }}>
        <h3 style={{ marginBottom: '0.5rem' }}>{sessionInfo.dbName}</h3>
        <button className="primary" style={{ width: '100%', marginBottom: '1rem' }} onClick={startNewChat}>+ New Chat</button>

        <div className="thread-list" style={{ display: 'flex', flexDirection: 'column', gap: '8px', overflowY: 'auto', flex: 1 }}>
          {initialLoading ? (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', marginTop: '2rem' }}>
              <div className="loader-spinner"></div>
              <div style={{ marginTop: '1rem', fontSize: '0.9rem', color: 'var(--text-muted)' }}>Loading history...</div>
            </div>
          ) : threads.length === 0 ? (
            <div style={{ textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.85rem', marginTop: '1rem' }}>
              No chat history.
            </div>
          ) : (
            threads.map((t) => (
              <div
                key={t.id}
                className={`thread-card animate-fade-in ${currentThreadId === t.id ? 'active' : ''}`}
                onClick={() => loadThread(t.id)}
                style={{
                  padding: '10px',
                  background: currentThreadId === t.id ? 'rgba(59, 130, 246, 0.1)' : 'rgba(255, 255, 255, 0.05)',
                  border: `1px solid ${currentThreadId === t.id ? '#3b82f6' : 'var(--border-color)'}`,
                  borderRadius: '4px',
                  cursor: 'pointer',
                  transition: 'background 0.2s'
                }}
              >
                <div style={{ fontSize: '0.9rem', fontWeight: 'bold', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                  {t.title}
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '4px' }}>
                  {(() => {
                    if (!t.created_at) return 'Just now';
                    const d = new Date(t.created_at);
                    if (isNaN(d.getTime())) return 'Invalid Date';
                    return new Intl.DateTimeFormat('en-IN', {
                      year: 'numeric', month: 'numeric', day: 'numeric',
                      hour: 'numeric', minute: 'numeric'
                    }).format(d);
                  })()}
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Main Chat Area */}
      <div className="chat-main glass-panel">
        <div className="chat-history">
          {history.length === 0 ? (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <h2 className="text-muted" style={{ opacity: 0.5 }}>Ask anything about your data...</h2>
            </div>
          ) : (
            history.map((msg, idx) => (
              <MessageBubble key={idx} msg={msg} idx={idx} />
            ))
          )}
          {loading && (
            <div className="message assistant animate-fade-in">
              <div className="message-bubble text-muted" style={{ fontStyle: 'italic' }}>
                Thinking<span className="dot-animate-1">.</span><span className="dot-animate-2">.</span><span className="dot-animate-3">.</span>
              </div>
              <style>{`
                @keyframes dotBlink {
                  0% { opacity: 0; }
                  20% { opacity: 1; }
                  100% { opacity: 0; }
                }
                .dot-animate-1 { animation: dotBlink 1.4s infinite linear; }
                .dot-animate-2 { animation: dotBlink 1.4s infinite linear 0.2s; }
                .dot-animate-3 { animation: dotBlink 1.4s infinite linear 0.4s; }
              `}</style>
            </div>
          )}
          <div ref={endRef} />
        </div>

        <form className="input-area" onSubmit={handleSend}>
          <input
            type="text"
            placeholder={initialLoading ? "Loading database schema..." : "e.g. Total sales grouped by region this month..."}
            value={inputData}
            onChange={(e) => setInputData(e.target.value)}
            disabled={loading || initialLoading}
            autoFocus
          />
          <button type="submit" className="send-btn" disabled={!inputData.trim() || loading || initialLoading}>
            {loading ? '...' : 'Send'}
          </button>
        </form>
      </div>
    </div>
  );
}

export default Chat;
