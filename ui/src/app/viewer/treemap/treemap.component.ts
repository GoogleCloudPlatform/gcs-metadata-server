import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild,
} from '@angular/core';
import {
  ChartType,
  GoogleChartComponent,
  GoogleChartsModule,
  Row,
} from 'angular-google-charts';
import { MetadataObject } from '../../services/explore.service';

@Component({
  selector: 'app-treemap',
  standalone: true,
  imports: [GoogleChartsModule],
  templateUrl: './treemap.component.html',
  styleUrl: './treemap.component.css',
})
export class TreemapComponent implements OnChanges, AfterViewInit {
  @Input({ required: true }) directoryList$!: MetadataObject[];
  @ViewChild('chart') chart!: GoogleChartComponent;

  @Output() newPathEvent = new EventEmitter<string>();
  @Output() onBackEvent = new EventEmitter<string>();

  cols = [
    { type: 'string', id: 'name' },
    { type: 'string', id: 'parent' },
    { type: 'number', id: 'size' },
  ];
  data: Row[] = [];
  type: ChartType = ChartType.TreeMap;
  opts = {
    enableHighlight: true,
    maxDepth: 1,
    maxPostDepth: 1,
    minColor: '#f00',
    midColor: '#ddd',
    maxColor: '#0d0',
    headerHeight: 15,
    fontColor: 'black',
    showScale: true,
    useWeightedAverageForAggregation: true,
    eventsConfig: {
      highlight: ['click'],
      unhighlight: ['mouseout'],
      rollup: ['contextmenu'],
    },
  };

  ngAfterViewInit(): void {
    if (this.chart) {
      google.visualization.events.addListener(
        this.chart.chartWrapper.getChart(),
        'select',
        this.handleChartClick.bind(this)
      );
    }
  }

  handleChartClick(e: any) {
    console.log(e);
  }

  ngOnChanges(changes: SimpleChanges): void {
    console.log('refreshing');
    this.refreshTreeMapData(changes['directoryList$'].currentValue);
  }

  onSelect(event: any) {
    const row = event.selection[0].row;
    const selectedDirName = this.data[row][0] as string;

    this.newPathEvent.emit(selectedDirName);
  }

  refreshTreeMapData(newDirs: MetadataObject[]) {
    this.data = [];
    const top3 = newDirs.slice(1, 4);

    const otherSize = newDirs.slice(4).reduce((sum, dir) => sum + dir.size, 0);
    const otherCount = newDirs
      .slice(4)
      .reduce((count, dir) => count + dir.count, 0);

    const parentDir = newDirs[0].name;

    const otherDir: MetadataObject = {
      name: 'Other',
      count: 0,
      parent: parentDir,
      size: otherSize,
    };

    // get the children of other
    const otherChildren = newDirs
      .slice(4)
      .map((dir) => {
        dir.parent = otherDir.name
        return [dir.name, dir.parent, 0.1]
      });

    const top3Sizes = [60, 25, 10];

    this.data = [
      [newDirs[0].name, null, newDirs[0].size], // parent
      ...top3.map((dir, i) => [dir.name, parentDir, top3Sizes[i]] as Row), // top 3 directories in size
      [otherDir.name, parentDir, 5] as Row, // group of other directories
      ...otherChildren,
    ];
  }
}
