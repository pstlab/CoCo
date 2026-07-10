import { h, VNode } from 'snabbdom';
import { coco } from '../coco';
import { flick } from '@ratiosolver/flick';
import mermaid from 'mermaid';
import svgPanZoom from 'svg-pan-zoom';
import { CoCoClass } from './class';

mermaid.initialize({ startOnLoad: false, theme: 'default' });

export function class_diagram(coco: coco.CoCo): VNode {
  if (coco.get_classes().size === 0) {
    return h('div', 'No classes available to display.');
  }

  let mermaidText = 'classDiagram\n';
  coco.get_classes().forEach((cls) => {
    mermaidText += `  class ${cls.get_name()} {\n`;
    cls.get_static_properties().forEach((prop, name) => {
      if (!prop.type.startsWith('object')) {
        mermaidText += `    +${name} : ${prop.type.endsWith('-array') ? prop.type.replace('-array', '[]') : prop.type}$\n`;
      }
    });
    cls.get_dynamic_properties().forEach((prop, name) => {
      if (!prop.type.startsWith('object')) {
        mermaidText += `    +${name} : ${prop.type.endsWith('-array') ? prop.type.replace('-array', '[]') : prop.type}\n`;
      }
    });
    mermaidText += '  }\n';
    cls.get_parents().forEach((parent) => {
      mermaidText += `  ${parent} <|-- ${cls.get_name()}\n`;
    });
    cls.get_static_properties().forEach((prop, name) => {
      if (prop.type === 'object') {
        prop.classes.forEach((targetClass) => {
          mermaidText += `  ${cls.get_name()} --> "1" ${targetClass} : ${name}\n`;
        });
      } else if (prop.type === 'object-array') {
        prop.classes.forEach((targetClass) => {
          mermaidText += `  ${cls.get_name()} --> "*" ${targetClass} : ${name}\n`;
        });
      }
    });
    cls.get_dynamic_properties().forEach((prop, name) => {
      if (prop.type === 'object') {
        prop.classes.forEach((targetClass) => {
          mermaidText += `  ${cls.get_name()} --> "1" ${targetClass} : ${name}\n`;
        });
      } else if (prop.type === 'object-array') {
        prop.classes.forEach((targetClass) => {
          mermaidText += `  ${cls.get_name()} --> "*" ${targetClass} : ${name}\n`;
        });
      }
    });
  });

  const uniqueId = 'mermaid-class-diagram';
  return h('div', {
    attrs: { id: uniqueId },
    class: { 'flex-grow-1': true, 'd-flex': true, 'flex-column': true },
    hook: {
      insert: (vnode) => {
        const containerElement = vnode.elm as HTMLElement;
        mermaid.render(`${uniqueId}-svg`, mermaidText, vnode.elm as HTMLElement)
          .then(({ svg }) => {
            containerElement.innerHTML = svg;

            const svgElement = containerElement.querySelector('svg');
            if (svgElement) {
              svgElement.style.maxWidth = 'none';
              svgElement.style.width = '100%';
              svgElement.style.height = '100%';

              svgElement.style.display = 'block';
              svgElement.style.flexGrow = '1';

              svgElement.style.cursor = 'grab';

              const style = document.createElement('style');
              style.innerHTML = 'g.node { cursor: pointer; }';
              svgElement.appendChild(style);

              let startX = 0;
              let startY = 0;
              const dragThreshold = 5;

              svgElement.addEventListener('mousedown', (event) => {
                startX = event.clientX;
                startY = event.clientY;
                svgElement.style.cursor = 'grabbing';
              });

              svgElement.addEventListener('mouseup', (event) => {
                const deltaX = Math.abs(event.clientX - startX);
                const deltaY = Math.abs(event.clientY - startY);
                if (deltaX < dragThreshold && deltaY < dragThreshold) {
                  const node = (event.target as SVGElement).closest('g.node');
                  if (node) {
                    const match = node.id.match(/-classId-([^-]+)/);
                    if (match && match[1]) {
                      const cls = coco.get_class(match[1]);
                      if (cls) {
                        flick.ctx.current_page = () => CoCoClass(cls);
                        flick.ctx.page_title = `Class: ${cls.get_name()}`;
                        flick.redraw();
                      }
                    }
                  }
                }
                svgElement.style.cursor = 'grab';
              });

              svgPanZoom(svgElement, {
                zoomEnabled: true,
                fit: true,
                center: true,
                minZoom: 0.1,
                maxZoom: 10,
              });
            }
          })
          .catch((err) => {
            console.error('Error rendering Mermaid diagram:', err);
            containerElement.innerHTML = `<p style='color: red;'>Error rendering Mermaid diagram: ${err.message}</p>`;
          });
      }
    }
  }, 'Generating class diagram...');
}