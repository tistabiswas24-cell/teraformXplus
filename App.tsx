import { useEffect, useState, useMemo } from "react";
import ReactECharts from "echarts-for-react";
import { Card, Elevation, Tag, Intent } from "@blueprintjs/core";
import ReactFlow, { Background, Controls } from "reactflow";
import "reactflow/dist/style.css";
import "@blueprintjs/core/lib/css/blueprint.css";

interface Telemetry {
  server_id: string;
  cpu_usage: number;
  latency_ms: number;
  status: string;
  time: string;
}

export default function App() {
  const [dataStream, setDataStream] = useState<Telemetry[]>([]);
  const [currentStatus, setCurrentStatus] = useState<string>("WAITING");

  useEffect(() => {
    const ws = new WebSocket("ws://127.0.0.1:8000/ws/telemetry");

    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      const timestamp = new Date().toLocaleTimeString();
      
      const newPoint = {
        server_id: payload.server_id,
        cpu_usage: payload.cpu_usage,
        latency_ms: payload.latency_ms,
        status: payload.status,
        time: timestamp,
      };

      setCurrentStatus(payload.status);
      setDataStream((prev) => [...prev, newPoint].slice(-30));
    };

    return () => ws.close();
  }, []);

  // --- React Flow Topology Map Logic ---
  const isCritical = currentStatus === "CRITICAL";

  const nodes = useMemo(() => [
    {
      id: "api-gateway",
      position: { x: 350, y: 20 },
      data: { label: "API Gateway (Load Balancer)" },
      style: { background: "#e1e8ed", fontWeight: "bold", width: 200 },
    },
    {
      id: "db-primary",
      position: { x: 150, y: 150 },
      data: { label: "aws-prod-db-01 (Primary)" },
      style: {
        background: isCritical ? "#ff7373" : "#b3ffd6", // Red if critical, Green if healthy
        border: isCritical ? "2px solid #a82a2a" : "1px solid #0f9960",
        color: isCritical ? "white" : "black",
        width: 200,
      },
    },
    {
      id: "db-standby",
      position: { x: 550, y: 150 },
      data: { label: "aws-prod-db-02 (Standby)" },
      style: {
        background: isCritical ? "#b3ffd6" : "#f5f8fa", // Activates during failover
        border: isCritical ? "2px solid #0f9960" : "1px dashed #5c7080",
        color: "black",
        width: 200,
      },
    },
  ], [isCritical]);

  const edges = useMemo(() => [
    {
      id: "edge-primary",
      source: "api-gateway",
      target: "db-primary",
      animated: !isCritical, // Traffic flows when healthy
      style: { stroke: isCritical ? "#ff7373" : "#0f9960", strokeWidth: 3 },
      label: isCritical ? "CONNECTION LOST" : "ACTIVE",
    },
    {
      id: "edge-standby",
      source: "api-gateway",
      target: "db-standby",
      animated: isCritical, // Traffic routes here during failover
      style: { stroke: isCritical ? "#0f9960" : "#8a9ba8", strokeWidth: isCritical ? 3 : 1 },
      label: isCritical ? "FAILOVER ACTIVE" : "STANDBY",
    },
  ], [isCritical]);

  // --- ECharts Configuration ---
  const option = {
    title: { text: "Live Infrastructure Telemetry", left: "center" },
    tooltip: { trigger: "axis" },
    legend: { data: ["CPU Usage (%)", "Latency (ms)"], bottom: 0 },
    grid: { left: "3%", right: "4%", bottom: "10%", containLabel: true },
    xAxis: {
      type: "category",
      boundaryGap: false,
      data: dataStream.map((d) => d.time),
    },
    yAxis: [
      { type: "value", name: "CPU (%)", max: 100 },
      { type: "value", name: "Latency (ms)", alignTicks: true }
    ],
    series: [
      {
        name: "CPU Usage (%)",
        type: "line",
        data: dataStream.map((d) => d.cpu_usage),
        itemStyle: { color: "#d9822b" },
        smooth: true,
      },
      {
        name: "Latency (ms)",
        type: "line",
        yAxisIndex: 1,
        data: dataStream.map((d) => d.latency_ms),
        itemStyle: { color: "#0f9960" },
        smooth: true,
      },
    ],
  };

  return (
    <div style={{ padding: "20px", backgroundColor: "#f5f8fa", minHeight: "100vh" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px" }}>
        <h2 style={{ margin: 0 }}>Disaster Recovery Command Center</h2>
        <Tag intent={isCritical ? Intent.DANGER : currentStatus === "HEALTHY" ? Intent.SUCCESS : Intent.NONE} large>
          SYSTEM STATUS: {currentStatus}
        </Tag>
      </div>

      {/* React Flow Topology Map */}
      <Card elevation={Elevation.TWO} style={{ marginBottom: "20px", height: "300px", padding: 0 }}>
        <ReactFlow nodes={nodes} edges={edges} fitView>
          <Background color="#ccc" gap={16} />
          <Controls />
        </ReactFlow>
      </Card>

      {/* Live Telemetry Chart */}
      <Card elevation={Elevation.TWO}>
        <ReactECharts option={option} style={{ height: "400px", width: "100%" }} />
      </Card>
    </div>
  );
}