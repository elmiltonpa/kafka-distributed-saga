import React, { useEffect, useState } from "react";
import { io } from "socket.io-client";
import "./App.css";

type SimulationFailureMode = "none" | "billing" | "provisioning";

interface TimelineEvent {
  activationId: string;
  customerId: string;
  planId: string;
  eventType: string;
  status: string;
  details?: Record<string, unknown>;
  timestamp: string;
}

interface ActivationSummary {
  activationId: string;
  customerId: string;
  planId: string;
  status: string;
  events: TimelineEvent[];
}

const API_BASE_URL = "http://localhost:3000";

export function App() {
  const [isConnected, setIsConnected] = useState(false);

  const [customerId, setCustomerId] = useState("C-1234");
  const [planId, setPlanId] = useState("FLOW-FULL");
  const [simulateFailure, setSimulateFailure] =
    useState<SimulationFailureMode>("none");
  const [isLoading, setIsLoading] = useState(false);

  const [activations, setActivations] = useState<
    Record<string, ActivationSummary>
  >({});
  const [selectedActivationId, setSelectedActivationId] = useState<
    string | null
  >(null);

  useEffect(() => {
    fetch(`${API_BASE_URL}/activations`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(
        (
          data: Array<{
            activationId: string;
            customerId: string;
            planId: string;
            status: string;
            history?: Array<{
              eventType: string;
              at: string;
              details?: Record<string, unknown>;
            }>;
          }>,
        ) => {
          if (Array.isArray(data) && data.length > 0) {
            const loaded: Record<string, ActivationSummary> = {};
            for (const act of data) {
              loaded[act.activationId] = {
                activationId: act.activationId,
                customerId: act.customerId,
                planId: act.planId,
                status: act.status,
                events: (act.history || []).map((h) => ({
                  activationId: act.activationId,
                  customerId: act.customerId,
                  planId: act.planId,
                  eventType: h.eventType,
                  status: act.status,
                  details: h.details,
                  timestamp: h.at,
                })),
              };
            }
            setActivations((prev) => ({ ...loaded, ...prev }));
            setSelectedActivationId(
              (prev) => prev || data[data.length - 1].activationId,
            );
          }
        },
      )
      .catch((err) => {
        console.warn(
          "No se pudo cargar el historial inicial de activaciones:",
          err,
        );
      });
  }, []);

  useEffect(() => {
    const s = io(API_BASE_URL, {
      transports: ["websocket", "polling"],
    });

    s.on("connect", () => {
      setIsConnected(true);
    });

    s.on("disconnect", () => {
      setIsConnected(false);
    });

    s.on("activationUpdate", (update: TimelineEvent) => {
      setActivations((prev) => {
        const current = prev[update.activationId] || {
          activationId: update.activationId,
          customerId: update.customerId,
          planId: update.planId,
          status: update.status,
          events: [],
        };

        const alreadyExists = current.events.some(
          (e) =>
            e.eventType === update.eventType &&
            e.timestamp === update.timestamp,
        );

        return {
          ...prev,
          [update.activationId]: {
            ...current,
            status: update.status,
            events: alreadyExists
              ? current.events
              : [...current.events, update],
          },
        };
      });

      setSelectedActivationId((current) => current || update.activationId);
    });

    return () => {
      s.disconnect();
    };
  }, []);

  const handleSubmit = async (e: React.SyntheticEvent<HTMLFormElement>) => {
    e.preventDefault();
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/activations`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          customerId,
          planId,
          simulateFailure,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }

      const data = (await response.json()) as { activationId: string };
      setSelectedActivationId(data.activationId);
    } catch (error) {
      console.error("Error al contratar plan:", error);
      alert("Error al conectar con activation-api en http://localhost:3000");
    } finally {
      setIsLoading(false);
    }
  };

  const currentActivation = selectedActivationId
    ? activations[selectedActivationId]
    : null;

  const getEventBadge = (eventType: string) => {
    if (eventType.includes("Failed")) return "danger";
    if (eventType.includes("Cancelled")) return "warning";
    if (eventType.includes("Created") || eventType.includes("Completed"))
      return "success";
    return "info";
  };

  const getEventIcon = (eventType: string) => {
    switch (eventType) {
      case "ActivationRequested":
        return "📨";
      case "BillingAccountCreated":
        return "💳";
      case "BillingFailed":
        return "❌";
      case "ProvisioningCompleted":
        return "📶";
      case "ProvisioningFailed":
        return "⚠️";
      case "BillingAccountCancelled":
        return "🔄";
      case "ActivationCompleted":
        return "✅";
      case "ActivationFailed":
        return "🛑";
      default:
        return "🔹";
    }
  };

  return (
    <div className="poc-container">
      {/* Header */}
      <header className="poc-header">
        <div>
          <h1>⚡ POC Activación de Servicios con Kafka</h1>
          <p>
            Saga Coreografiada con Agregador · UTN FRCU (Sistemas Distribuidos
            2026)
          </p>
        </div>
        <div className="ws-status-badge">
          <span className={`ws-dot ${isConnected ? "online" : "offline"}`} />
          {isConnected ? "WebSocket Conectado (3000)" : "Desconectado"}
        </div>
      </header>

      {/* Main Grid */}
      <div className="poc-grid">
        {/* Left Column: Form */}
        <div className="poc-card">
          <h2>📝 Contratar Plan</h2>
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label>ID de Cliente (Partition Key)</label>
              <input
                type="text"
                className="form-control"
                value={customerId}
                onChange={(e) => setCustomerId(e.target.value)}
                required
              />
              <div className="quick-tags">
                <button
                  type="button"
                  className="quick-btn"
                  onClick={() => setCustomerId("C-1001")}
                >
                  C-1001
                </button>
                <button
                  type="button"
                  className="quick-btn"
                  onClick={() => setCustomerId("C-1002")}
                >
                  C-1002
                </button>
                <button
                  type="button"
                  className="quick-btn"
                  onClick={() => setCustomerId("C-2000")}
                >
                  C-2000
                </button>
              </div>
            </div>

            <div className="form-group">
              <label>Plan de Servicio</label>
              <select
                className="form-control"
                value={planId}
                onChange={(e) => setPlanId(e.target.value)}
              >
                <option value="FLOW-FULL">
                  FLOW-FULL (Fibra 300M + TV + Móvil)
                </option>
                <option value="FIBRA-1000M">
                  FIBRA-1000M (Fibra Óptica 1 Gbps)
                </option>
                <option value="MOVIL-5G">
                  MOVIL-5G (Datos Móviles Ilimitados)
                </option>
              </select>
            </div>

            <div className="form-group">
              <label>Simulación de Fallo (RF-01)</label>
              <select
                className="form-control"
                value={simulateFailure}
                onChange={(e) =>
                  setSimulateFailure(e.target.value as SimulationFailureMode)
                }
              >
                <option value="none">
                  🟢 Sin fallos (Camino Feliz - Fan-out)
                </option>
                <option value="billing">🔴 Forzar fallo en Facturación</option>
                <option value="provisioning">
                  ⚠️ Forzar fallo en Aprovisionamiento (Compensación)
                </option>
              </select>
            </div>

            <button type="submit" className="btn-primary" disabled={isLoading}>
              {isLoading ? "Enviando a Kafka..." : "🚀 Iniciar Activación"}
            </button>
          </form>

          {/* Quick Links */}
          <div className="external-links">
            <a
              href="http://localhost:8080"
              target="_blank"
              rel="noreferrer"
              className="ext-link"
            >
              <span>📊 Kafka UI (kafbat)</span>
              <span>:8080 ↗</span>
            </a>
            <a
              href="http://localhost:8025"
              target="_blank"
              rel="noreferrer"
              className="ext-link"
            >
              <span>📬 Mailhog (Bandeja de Entrada)</span>
              <span>:8025 ↗</span>
            </a>
          </div>

          {/* Previous Activations */}
          {Object.keys(activations).length > 0 && (
            <div style={{ marginTop: "20px" }}>
              <label
                style={{ fontSize: "12px", color: "#94a3b8", fontWeight: 600 }}
              >
                HISTORIAL DE ACTIVACIONES ({Object.keys(activations).length})
              </label>
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  gap: "6px",
                  marginTop: "8px",
                }}
              >
                {Object.values(activations).map((act) => (
                  <button
                    key={act.activationId}
                    type="button"
                    onClick={() => setSelectedActivationId(act.activationId)}
                    style={{
                      background:
                        selectedActivationId === act.activationId
                          ? "#334155"
                          : "#0f172a",
                      border: "1px solid #334155",
                      padding: "8px 10px",
                      borderRadius: "6px",
                      color: "#fff",
                      textAlign: "left",
                      cursor: "pointer",
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <span>
                      {act.activationId} ({act.customerId})
                    </span>
                    <span
                      className={`status-badge ${act.status}`}
                      style={{ fontSize: "10px" }}
                    >
                      {act.status}
                    </span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Right Column: Timeline */}
        <div className="poc-card">
          <div className="timeline-header">
            <div>
              <h2 style={{ border: "none", margin: 0, padding: 0 }}>
                📡 Timeline en Tiempo Real (WebSocket)
              </h2>
              {currentActivation && (
                <p
                  style={{
                    color: "#94a3b8",
                    fontSize: "13px",
                    marginTop: "4px",
                  }}
                >
                  Activación: <strong>{currentActivation.activationId}</strong>{" "}
                  | Cliente: <strong>{currentActivation.customerId}</strong> |
                  Plan: <strong>{currentActivation.planId}</strong>
                </p>
              )}
            </div>
            {currentActivation && (
              <span className={`status-badge ${currentActivation.status}`}>
                {currentActivation.status}
              </span>
            )}
          </div>

          {!currentActivation || currentActivation.events.length === 0 ? (
            <div className="empty-timeline">
              <p style={{ fontSize: "32px", marginBottom: "8px" }}>⏳</p>
              <p>No hay eventos registrados todavía.</p>
              <p style={{ fontSize: "13px" }}>
                Contratá un plan desde el formulario para ver los eventos fluir
                en vivo.
              </p>
            </div>
          ) : (
            <div className="timeline">
              {currentActivation.events.map((ev, index) => {
                const badgeType = getEventBadge(ev.eventType);
                const icon = getEventIcon(ev.eventType);

                return (
                  <div
                    key={`${ev.eventType}-${index}`}
                    className="timeline-item"
                  >
                    <div className={`timeline-dot ${badgeType}`} />
                    <div className="timeline-item-header">
                      <span className="event-title">
                        <span>{icon}</span>
                        <span>{ev.eventType}</span>
                      </span>
                      <span className="event-time">
                        {new Date(ev.timestamp).toLocaleTimeString()}
                      </span>
                    </div>

                    <div className="event-details">
                      {ev.eventType === "ActivationRequested" && (
                        <span>
                          Publicado por <code>activation-api</code> en el tópico{" "}
                          <code>activation.requested</code>.
                        </span>
                      )}
                      {ev.eventType === "BillingAccountCreated" && (
                        <span>
                          Cuenta facturada creada por{" "}
                          <code>billing-service</code>. ID:{" "}
                          <code>
                            {String(ev.details?.billingAccountId ?? "")}
                          </code>
                        </span>
                      )}
                      {ev.eventType === "BillingFailed" && (
                        <span style={{ color: "#ef4444" }}>
                          Fallo en facturación:{" "}
                          {String(ev.details?.reason ?? "")}
                        </span>
                      )}
                      {ev.eventType === "ProvisioningCompleted" && (
                        <span>
                          Línea y red aprovisionada por{" "}
                          <code>provisioning-service</code> tras demora técnica.
                        </span>
                      )}
                      {ev.eventType === "ProvisioningFailed" && (
                        <span style={{ color: "#ef4444" }}>
                          Fallo en red física:{" "}
                          {String(ev.details?.reason ?? "")}
                        </span>
                      )}
                      {ev.eventType === "BillingAccountCancelled" && (
                        <span style={{ color: "#f59e0b" }}>
                          Compensación de la Saga: <code>billing-service</code>{" "}
                          anuló la cuenta{" "}
                          <code>
                            {String(ev.details?.billingAccountId ?? "")}
                          </code>
                          .
                        </span>
                      )}
                      {ev.eventType === "ActivationCompleted" && (
                        <span style={{ color: "#10b981", fontWeight: 600 }}>
                          ¡Saga completada! Ambos servicios dieron OK. Email de
                          bienvenida despachado a Mailhog.
                        </span>
                      )}
                      {ev.eventType === "ActivationFailed" && (
                        <span style={{ color: "#ef4444", fontWeight: 600 }}>
                          Saga abortada. Se notificó al cliente el motivo del
                          error.
                        </span>
                      )}
                    </div>

                    {ev.details && Object.keys(ev.details).length > 0 && (
                      <pre className="json-preview">
                        {JSON.stringify(ev.details, null, 2)}
                      </pre>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
