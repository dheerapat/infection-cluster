import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import * as d3 from "d3"

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './app.html',
  styleUrls: ['./app.css']
})
export class App {
  protected readonly title = signal('cluster-vis');

  transferFile: File | null = null;
  microFile: File | null = null;
  result: any = null;
  loading = false;

  constructor(private http: HttpClient) { }

  onTransferFile(event: Event) {
    const input = event.target as HTMLInputElement;
    this.transferFile = input.files?.[0] ?? null;
  }

  onMicroFile(event: Event) {
    const input = event.target as HTMLInputElement;
    this.microFile = input.files?.[0] ?? null;
  }

  submit() {
    if (!this.transferFile || !this.microFile) {
      alert("Please upload both files");
      return;
    }
    this.loading = true;

    const formData = new FormData();
    formData.append("transfer_file", this.transferFile);
    formData.append("micro_file", this.microFile);

    this.http.post("http://localhost:8000/cluster", formData).subscribe({
      next: (res) => {
        this.result = res;
        this.loading = false;
        this.drawGraph();
      },
      error: (err) => {
        console.error(err);
        this.loading = false;
      }
    });
  }

  drawGraph() {
    if (!this.result) return;
    const data = this.result;
    d3.select("#graph").select("svg").remove();

    const width = 900, height = 900;

    const svg = d3.select("#graph")
      .append("svg")
      .attr("width", width)
      .attr("height", height);

    const container = svg.append("g");

    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 10])
      .on("zoom", (event) => {
        container.attr("transform", event.transform);
      });

    svg.call(zoom);

    const simulation = d3.forceSimulation(data.nodes)
      .force("link", d3.forceLink(data.edges)
        .id((d: any) => d.id)
        .distance(50)
        .strength(0.8)
      )
      .force("charge", d3.forceManyBody()
        .strength(-100)
        .distanceMax(200)
      )
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide()
        .radius(15)
        .strength(0.7)
      )
      .alpha(1)
      .alphaDecay(0.02)
      .velocityDecay(0.4);

    const link = container.append("g")
      .selectAll("line")
      .data(data.edges)
      .enter().append("line")
      .attr("stroke", "#999")
      .attr("stroke-width", (d: any) => Math.sqrt(d.value || 1))
      .attr("stroke-opacity", 0.6);

    const node = container.append("g")
      .selectAll("circle")
      .data(data.nodes)
      .enter().append("circle")
      .attr("r", 8)
      .attr("fill", (d: any) => {
        const colors = d3.schemeCategory10;
        return colors[d.group % colors.length] || "#69b3a2";
      })
      .attr("stroke", "#fff")
      .attr("stroke-width", 1.5)
      .call(d3.drag<SVGCircleElement, any>()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragended));

    const label = container.append("g")
      .selectAll("text")
      .data(data.nodes)
      .enter().append("text")
      .text((d: any) => d.id)
      .attr("font-size", 10)
      .attr("dx", 10)
      .attr("dy", ".35em")
      .attr("font-family", "Arial, sans-serif")
      .attr("fill", "#333");

    simulation.on("tick", () => {
      link
        .attr("x1", (d: any) => d.source.x)
        .attr("y1", (d: any) => d.source.y)
        .attr("x2", (d: any) => d.target.x)
        .attr("y2", (d: any) => d.target.y);

      node
        .attr("cx", (d: any) => d.x)
        .attr("cy", (d: any) => d.y);

      label
        .attr("x", (d: any) => d.x)
        .attr("y", (d: any) => d.y);
    });

    function dragstarted(event: any, d: any) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      d.fx = d.x;
      d.fy = d.y;
    }

    function dragged(event: any, d: any) {
      d.fx = event.x;
      d.fy = event.y;
    }

    function dragended(event: any, d: any) {
      if (!event.active) simulation.alphaTarget(0);
      d.fx = null;
      d.fy = null;
    }

    // Reset zoom on double-click
    svg.on("dblclick.zoom", () => {
      svg.transition()
        .duration(750)
        .call(zoom.transform, d3.zoomIdentity);
    });
  }
}
