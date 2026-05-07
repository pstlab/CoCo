import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import { flick, Header, ListGroup, Row, Table } from "@ratiosolver/flick";
import { CoCoClass } from "./class";
import * as echarts from 'echarts/core';
import { LineChart, CustomChart } from 'echarts/charts';
import { LegendComponent, TooltipComponent, GridComponent, DataZoomComponent, AxisPointerComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { CustomSeriesRenderItemAPI, CustomSeriesRenderItemParams } from 'echarts/types/dist/shared';

echarts.use([LineChart, CustomChart, LegendComponent, TooltipComponent, GridComponent, DataZoomComponent, AxisPointerComponent, CanvasRenderer]);

const PIXELS_PER_ROW = 150;
const BOTTOM_UI_HEIGHT = 50;

const obj_item_listener: coco.CoCoObjectListener = {
  class_added: (_cls: coco.CoCoClass) => { },
  properties_updated: (properties: Record<string, coco.Value>) => { if (properties.name) flick.redraw(); },
  values_added: (_values: Record<string, coco.Value>, _date_time: string) => { },
  data_updated: (_data: Record<string, Array<coco.TimeValue>>) => { }
};

export function ObjectGroupItem(obj: coco.CoCoObject): VNode {
  const active = flick.ctx.page_title === `Object: ${obj.get_id()}`;
  return h('button.list-group-item.list-group-item-action' + (active ? '.active.rounded' : ''), {
    hook: {
      insert: () => {
        obj.add_listener(obj_item_listener);
      },
      destroy: () => {
        obj.remove_listener(obj_item_listener);
      }
    },
    props: { type: 'button' },
    attrs: { 'aria-current': active ? 'true' : 'false' },
    on: {
      click: () => {
        flick.ctx.current_page = () => CoCoObject(obj);
        flick.ctx.page_title = `Object: ${obj.get_id()}`;
        flick.redraw();
      }
    }
  }, object_to_string(obj));
}

export function ObjectsList(coco: coco.CoCo): VNode {
  return ListGroup(Array.from(coco.get_objects().values().map(obj => ObjectGroupItem(obj))));
}

export function CoCoObject(obj: coco.CoCoObject): VNode {
  let chart: echarts.ECharts | undefined;
  const all_props = new Map<string, coco.Property>();
  obj.get_classes().forEach(cls => {
    cls.get_dynamic_properties().forEach((prop, name) => {
      all_props.set(name, prop);
    });
  });

  const get_option = (): echarts.EChartsCoreOption => {
    if (!obj.is_data_loaded())
      obj.load_data();

    const data = obj.get_data();
    const all_timestamps: number[] = [];
    Object.values(data).forEach(seriesData => seriesData.forEach(d => all_timestamps.push(new Date(d.timestamp).getTime())));

    const global_min = all_timestamps.length ? Math.min(...all_timestamps) : undefined;
    const global_max = all_timestamps.length ? Math.max(...all_timestamps) : new Date().getTime();

    const toArrayLabel = (v: coco.Value): string => Array.isArray(v) ? `[${v.map(x => coco.value_to_string(x)).join(', ')}]` : coco.value_to_string(v);

    type ChartSeriesRow = {
      yAxis: Record<string, unknown>;
      series: Record<string, unknown> | Record<string, unknown>[];
    };

    const series: ChartSeriesRow[] = Array.from(all_props.entries()).flatMap(([name, prop], index): ChartSeriesRow[] => {
      switch (prop.type) {
        case 'int':
        case 'float':
          return [{
            yAxis: {
              type: 'value',
              gridIndex: index,
              name,
              min: prop.min ? prop.min as number : undefined,
              max: prop.max ? prop.max as number : undefined,
              splitLine: { show: true }
            },
            series: {
              type: 'line',
              name,
              xAxisIndex: index,
              yAxisIndex: index,
              showSymbol: false,
              data: data[name]?.map(d => [d.timestamp, d.value as number]) || []
            }
          }];
        case 'int-array':
        case 'float-array': {
          const snapshots = (data[name] ?? []).map(d => ({
            t: d.timestamp,
            arr: Array.isArray(d.value) ? d.value : []
          }));
          const maxLen = snapshots.reduce((m, s) => Math.max(m, s.arr.length), 0);

          return [{
            yAxis: {
              type: 'value',
              gridIndex: index,
              name,
              min: prop.min ? prop.min as number : undefined,
              max: prop.max ? prop.max as number : undefined,
              splitLine: { show: true }
            },
            series: Array.from({ length: maxLen }, (_, arrIdx) => ({
              type: 'line',
              name: `${name}[${arrIdx}]`,
              xAxisIndex: index,
              yAxisIndex: index,
              showSymbol: false,
              data: snapshots
                .filter(s => typeof s.arr[arrIdx] === 'number')
                .map(s => [s.t, s.arr[arrIdx] as number])
            }))
          }];
        }
        case 'bool':
        case 'string':
        case 'symbol':
        case 'object':
        case 'bool-array':
        case 'string-array':
        case 'symbol-array':
        case 'object-array':
          const c_data: { start: string; end: string; value: string }[] = [];
          let current_value: string | null = null;
          let current_start: string | null = null;

          for (const d of data[name] || []) {
            const value_str = toArrayLabel(d.value);
            if (current_value !== null && current_start !== null) {
              c_data.push({ start: current_start, end: d.timestamp, value: current_value });
            }
            current_value = value_str;
            current_start = d.timestamp;
          }

          if (current_value !== null && current_start !== null) {
            c_data.push({
              start: current_start,
              end: new Date((global_max ?? Date.now()) + 1).toISOString(),
              value: current_value
            });
          }

          const colorMap: Record<string, string> = {};
          const uniqueValues = [...new Set(c_data.map(d => d.value))];
          const colorPalette = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc'];
          uniqueValues.forEach((val, i) => { colorMap[val] = colorPalette[i % colorPalette.length]; });

          return [{
            yAxis: {
              type: 'value',
              gridIndex: index,
              name,
              splitLine: { show: true }
            },
            series: {
              name,
              type: 'custom',
              xAxisIndex: index,
              yAxisIndex: index,
              renderItem: (params: CustomSeriesRenderItemParams, api: CustomSeriesRenderItemAPI) => {
                const coordSys = params.coordSys as unknown as {
                  x: number;
                  y: number;
                  width: number;
                  height: number;
                };

                const start = api.coord([api.value(0), 0]);
                const end = api.coord([api.value(1), 0]);
                const color = api.value(3) as string;

                return {
                  type: 'rect',
                  shape: {
                    x: start[0],
                    y: coordSys.y + 2,
                    width: Math.max(0, end[0] - start[0]), // Ensure width isn't negative
                    height: coordSys.height - 4,
                    r: 2
                  },
                  style: {
                    fill: color
                  }
                };
              },
              encode: { x: [0, 1], y: 2 },
              data: c_data.map(d => [new Date(d.start).getTime(), new Date(d.end).getTime(), d.value, colorMap[d.value]])
            }
          }];
      }
    });

    return {
      axisPointer: { link: [{ xAxisIndex: 'all' }] },
      tooltip: {
        trigger: 'item'
      },
      dataZoom: [
        {
          type: 'slider',
          xAxisIndex: 'all',
        },
        {
          type: 'inside',
          xAxisIndex: 'all'
        }
      ],
      grid: series.map((_, i) => ({
        left: 20,
        right: 10,
        top: (i * PIXELS_PER_ROW),
        height: PIXELS_PER_ROW - 40,
      })),
      xAxis: series.map((_, i) => ({
        type: 'time',
        gridIndex: i,
        min: global_min,
        max: global_max,
        show: i === all_props.size - 1,
      })),
      yAxis: series.map(serie => serie.yAxis),
      series: series.flatMap(serie => Array.isArray(serie.series) ? serie.series : [serie.series]),
    };
  }

  const obj_listener = {
    class_added: (_cls: coco.CoCoClass) => { flick.redraw(); },
    properties_updated: (_properties: Record<string, coco.Value>) => { flick.redraw(); },
    values_added: (_values: Record<string, coco.Value>, _date_time: string) => { flick.redraw(); if (chart) chart.setOption(get_option()); },
    data_updated: (_data: Record<string, Array<coco.TimeValue>>) => { flick.redraw(); if (chart) chart.setOption(get_option()); }
  };

  let resize_handler: () => void;

  const props_header = ["Property", "Value"];
  const props = obj.get_properties();
  const props_rows = props ? Object.entries(props).map(([name, value]) => Row([name, coco.value_to_string(value)])) : [];

  const content = h('div.container.mt-2', [
    h('div.input-group', [
      h('input.form-control', { attrs: { type: 'text', value: obj.get_id(), placeholder: 'Type name', disabled: true } }),
      h('button.btn.btn-outline-secondary', {
        attrs: { type: 'button', title: 'Copy type name to clipboard' },
        on: { click: () => navigator.clipboard.writeText(obj.get_id()) }
      }, h('i.fa-solid.fa-copy')),
    ]),
    h('div.mt-2', Array.from(obj.get_classes()).map(cls =>
      h('span.badge.bg-primary.me-1', {
        style: { cursor: 'pointer' },
        on: {
          click: () => {
            flick.ctx.current_page = () => CoCoClass(cls);
            flick.ctx.page_title = `Class: ${cls.get_name()}`;
            flick.redraw();
          }
        }
      }, cls.get_name())
    )),
    props_rows.length > 0 ? Table(Header(props_header), props_rows, 'Properties') : h('p.mt-2', 'No properties.'),
    h('div.mt-2', {
      key: obj.get_id(),
      style: { minHeight: `${(all_props.size * PIXELS_PER_ROW) + BOTTOM_UI_HEIGHT}px` },
      hook: {
        insert: (vnode) => {
          chart = echarts.init(vnode.elm as HTMLDivElement);
          chart.setOption(get_option());

          resize_handler = () => chart?.resize();
          window.addEventListener('resize', resize_handler);

          obj.add_listener(obj_listener);
        },
        destroy: () => {
          window.removeEventListener('resize', resize_handler);
          obj.remove_listener(obj_listener);
          if (chart) {
            chart.dispose();
            chart = undefined;
          }
        }
      }
    }, 'Loading history...')
  ]);
  return content;
}

function object_to_string(obj: coco.CoCoObject): string {
  return obj.get_properties()?.name as string || obj.get_id();
}