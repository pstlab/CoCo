import { flick } from '@ratiosolver/flick';
import { coco } from '../src/coco';
import { CoCoApp } from '../src/components/app';
import '@fortawesome/fontawesome-free/css/all.css';

const cc = new coco.CoCo();

flick.mount(() => CoCoApp(cc));
