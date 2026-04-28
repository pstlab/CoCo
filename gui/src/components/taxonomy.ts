import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import * as echarts from 'echarts/core';
import { GraphChart } from "echarts/charts";
import { flick } from "@ratiosolver/flick";
import { CoCoClass } from "./class";

echarts.use([GraphChart]);

export function taxonomy(coco: coco.CoCo): VNode {
  let chart: echarts.ECharts | undefined;

  const get_option = (): echarts.EChartsCoreOption => {
    const classes = Array.from(coco.get_classes().values()).map(cls => ({ name: cls.get_name() }));
    const links: { source: string, target: string, lineStyle?: { type: 'dashed', dashOffset?: number } | { type: 'dotted', dashOffset?: number }, symbol?: [string, string] }[] = [];
    for (const cls of coco.get_classes().values()) {
      for (const parent of cls.get_parents())
        links.push({ source: cls.get_name(), target: parent, symbol: ['none', 'arrow'] });
      for (const target of cls.get_static_properties().values().filter(prop => prop.type === 'object').map(prop => prop.class as string))
        links.push({ source: cls.get_name(), target, lineStyle: { type: 'dotted', dashOffset: 5 }, symbol: ['none', 'circle'] });
      for (const target of cls.get_dynamic_properties().values().filter(prop => prop.type === 'object').map(prop => prop.class as string))
        links.push({ source: cls.get_name(), target, lineStyle: { type: 'dashed', dashOffset: 5 }, symbol: ['none', 'circle'] });
    }

    return {
      series: [
        {
          type: 'graph',
          layout: 'force',
          draggable: true,
          data: classes,
          links,
          roam: true,
          label: {
            show: true,
            position: 'right'
          },
          force: {
            repulsion: 100,
            edgeLength: 50,
            gravity: 0.1
          }
        }
      ]
    };
  };

  const connection_listener = {
    connection_error: (_error: Event) => { },
    connected: () => { },
    disconnected: () => { if (chart) chart.setOption(get_option()); },
  };

  const coco_listener = {
    initialized: () => { if (chart) chart.setOption(get_option()); },
    created_class: (_cls: coco.CoCoClass) => { if (chart) chart.setOption(get_option()); },
    created_object: (_obj: coco.CoCoObject) => { },
    created_rule: (_rule: coco.CoCoRule) => { },
  };

  let resize_handler: () => void;

  return h('div#taxonomy.flex-grow-1', {
    hook: {
      insert: (vnode) => {
        chart = echarts.init(vnode.elm as HTMLDivElement);
        chart.setOption(get_option());

        chart.on('click', 'series.graph', (params) => {
          if (params.dataType === 'node') {
            const data = params.data as { name: string };
            const cls = coco.get_class(data.name);
            if (cls) {
              flick.ctx.current_page = () => CoCoClass(cls);
              flick.ctx.page_title = `Class: ${cls.get_name()}`;
              flick.redraw();
            }
          }
        });

        resize_handler = () => chart?.resize();
        window.addEventListener('resize', resize_handler);

        coco.add_connection_listener(connection_listener);
        coco.add_listener(coco_listener);
      },
      destroy: () => {
        window.removeEventListener('resize', resize_handler);
        coco.remove_connection_listener(connection_listener);
        coco.remove_listener(coco_listener);
        if (chart) {
          chart.dispose();
          chart = undefined;
        }
      }
    }
  });
}