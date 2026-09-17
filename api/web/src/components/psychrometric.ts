import { h, VNode } from "snabbdom";
import { coco } from "../coco";
import * as echarts from 'echarts/core';
import { LineChart, ScatterChart } from 'echarts/charts';
import { GridComponent, TooltipComponent, TitleComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { flick } from "@ratiosolver/flick";

echarts.use([LineChart, ScatterChart, GridComponent, TooltipComponent, TitleComponent, CanvasRenderer]);

let from = Date.now() - 1000 * 60 * 60 * 24 * 14; // 2 weeks ago
let to = Date.now();

export type State = {
  timestamp: Date;
  temperature: number;
  relative_humidity: number;
};

export type Environment = {
  name: string;
  temperature_min: number;
  temperature_max: number;
  relative_humidity_min: number;
  relative_humidity_max: number;
};

const TEMP_START = -5;
const TEMP_END = 40;
const TEMP_INCREMENT = 1;

const STATIC_RH_LINES = Array.from({ length: 10 }).map((_, idx) => {
  const rh = (idx + 1) * 0.1;
  const lineData = [];
  for (let t = TEMP_START; t <= TEMP_END; t += TEMP_INCREMENT) {
    lineData.push([t, get_specific_humidity(t, rh)]);
  }
  return {
    name: `${Math.round(rh * 100)}% RH`,
    type: 'line',
    data: lineData,
    smooth: true,
    showSymbol: false,
    lineStyle: { color: 'rgba(0, 0, 0, 0.3)', width: 1 },
    endLabel: { show: true, formatter: '{a}', color: 'rgba(0, 0, 0, 0.7)' }
  };
});

export function psychrometric(obj: coco.CoCoObject): VNode {
  if (!Array.from(obj.get_classes()).some(cls => cls.get_name() === 'PsychrometricSensor')) {
    return h('div#psychrometric.flex-grow-1.d-flex.align-items-center.justify-content-center', {
      style: { minHeight: '600px', color: '#888' }
    }, 'This object is not a PsychrometricSensor');
  }

  let chart: echarts.ECharts | undefined;

  const get_option = (): echarts.EChartsCoreOption => {
    const envClass = obj.get_coco().get_class('PsychrometricEnvironment');
    const envInstances = envClass ? Array.from(envClass.get_instances().values()) : [];

    const environments: Environment[] = envInstances.map((env_obj: coco.CoCoObject) => {
      const props = env_obj.get_properties()!;
      return {
        name: props['name'] as string,
        temperature_min: props['min_temperature'] as number,
        temperature_max: props['max_temperature'] as number,
        relative_humidity_min: props['min_relative_humidity'] as number,
        relative_humidity_max: props['max_relative_humidity'] as number
      };
    });

    if (!obj.is_data_loaded())
      obj.load_data(from, to);

    const data = obj.get_data();

    const temps = data['temperature'] || [];
    const rhs = data['relative_humidity'] || [];

    const states: State[] = temps.reduce((acc: State[], t_data, index) => {
      const rh_data = rhs[index];
      if (rh_data) {
        let rh_val = rh_data.value as number;
        if (rh_val > 1) rh_val /= 100;
        acc.push({
          timestamp: new Date(t_data.timestamp),
          temperature: t_data.value as number,
          relative_humidity: rh_val
        });
      }
      return acc;
    }, []);

    const series: any[] = [...STATIC_RH_LINES];

    for (const env of environments) {
      const polygonData = [];
      for (let t = env.temperature_min; t <= env.temperature_max; t += TEMP_INCREMENT) {
        polygonData.push([t, get_specific_humidity(t, env.relative_humidity_max)]);
      }
      for (let t = env.temperature_max; t >= env.temperature_min; t -= TEMP_INCREMENT) {
        polygonData.push([t, get_specific_humidity(t, env.relative_humidity_min)]);
      }
      polygonData.push([env.temperature_min, get_specific_humidity(env.temperature_min, env.relative_humidity_max)]);

      series.push({
        name: env.name,
        type: 'line',
        data: polygonData,
        areaStyle: { opacity: 0.3 },
        lineStyle: { width: 2 },
        showSymbol: false
      });
    }

    const valid_states = states.filter(s => is_state_ok(s, environments)).map(s => [s.temperature, get_specific_humidity(s.temperature, s.relative_humidity)]);
    const invalid_states = states.filter(s => !is_state_ok(s, environments)).map(s => [s.temperature, get_specific_humidity(s.temperature, s.relative_humidity)]);

    series.push({
      name: `Valid`,
      type: 'scatter',
      data: valid_states,
      itemStyle: { color: 'rgba(0, 255, 0, 0.8)' }
    });

    series.push({
      name: `Invalid`,
      type: 'scatter',
      data: invalid_states,
      itemStyle: { color: 'rgba(255, 0, 0, 0.8)' }
    });

    return {
      title: { text: 'Psychrometric Chart' },
      tooltip: {
        trigger: 'item',
        formatter: (params: any) => {
          if (params.seriesType === 'scatter') {
            return `<b>Temp:</b> ${params.value[0]}°C<br><b>SH:</b> ${params.value[1].toFixed(2)} g/kg`;
          }
          return params.seriesName;
        }
      },
      xAxis: { type: 'value', name: 'Temperature (°C)', min: TEMP_START, max: TEMP_END },
      yAxis: { type: 'value', name: 'Specific Humidity (g/kg)' },
      series
    };
  }

  const obj_listener: coco.CoCoObjectListener = {
    classes_updated: (_classes: Set<coco.CoCoClass>) => { flick.redraw(); },
    properties_updated: (_properties: Record<string, coco.Value>) => { flick.redraw(); },
    values_added: (_values: Record<string, coco.Value>, _timestamp: string) => {
      flick.redraw();
      if (chart) chart.setOption(get_option());
    },
    data_updated: (_data: Record<string, Array<coco.TimeValue>>) => {
      flick.redraw();
      if (chart) chart.setOption(get_option());
    }
  };

  let resize_handler: () => void;

  return h('div#psychrometric.flex-grow-1', {
    style: { minHeight: '600px' },
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
  });
}

/**
 * Checks if the given state is valid in all provided environments.
 *
 * @param state - The state to check.
 * @param environments - An array of environments to validate the state against.
 * @returns `true` if the state is valid in all environments, otherwise `false`.
 */
export function is_state_ok(state: State, environments: Environment[]): boolean {
  for (const env of environments)
    if (!is_state_in_environment(state, env))
      return false;
  return true;
}

function is_state_in_environment(state: State, environment: Environment): boolean {
  if (state.temperature < environment.temperature_min || state.temperature > environment.temperature_max)
    return false;
  const sh = get_specific_humidity(state.temperature, state.relative_humidity);
  const sh_min = get_specific_humidity(state.temperature, environment.relative_humidity_min);
  const sh_max = get_specific_humidity(state.temperature, environment.relative_humidity_max);
  return sh >= sh_min && sh <= sh_max;
}

const PRESSURE = 101310;

/**
 * Calculates the saturation vapor pressure (SVP) in Pascals (Pa) for a given temperature.
 *
 * The formula used depends on whether the temperature is above or below 0°C:
 * - For temperatures >= 0°C, the formula is: `610.5 * Math.exp((17.269 * t) / (237.3 + t))`
 * - For temperatures < 0°C, the formula is: `610.5 * Math.exp((21.875 * t) / (265.5 + t))`
 *
 * @param {number} t - The temperature in degrees Celsius.
 * @returns {number} - The saturation vapor pressure in Pascals (Pa).
 */
export function get_saturation_vapor_pressure(t: number): number {
  if (t >= 0)
    return 610.5 * Math.exp((17.269 * t) / (237.3 + t));
  else
    return 610.5 * Math.exp((21.875 * t) / (265.5 + t));
}

/**
 * Calculates the specific humidity (SH) given the temperature and relative humidity.
 *
 * @param t - The temperature in degrees Celsius.
 * @param rh - The relative humidity as a fraction between 0 and 1.
 * @returns The specific humidity in grams per kilogram (g/kg).
 *
 * @remarks
 * This function uses the saturation vapor pressure (SVP) and the constant atmospheric pressure (PRESSURE)
 * to calculate the specific humidity. The SVP is obtained from the `get_saturation_vapor_pressure` function.
 */
export function get_specific_humidity(t: number, rh: number): number {
  const svp = get_saturation_vapor_pressure(t);
  return 0.622 * (svp * rh / (PRESSURE - svp)) * 1000;
}