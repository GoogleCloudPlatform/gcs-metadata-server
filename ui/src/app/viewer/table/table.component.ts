import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
} from '@angular/core';
import { MetadataObject } from '../../services/explore.service';
import { MatTableModule } from '@angular/material/table';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';

@Component({
  selector: 'app-table',
  standalone: true,
  imports: [MatTableModule, MatProgressSpinnerModule, MatIconModule],
  templateUrl: './table.component.html',
  styleUrl: './table.component.css',
})
export class TableComponent implements OnChanges {
  @Input({ required: true }) directoryList$!: MetadataObject[];
  @Input({ required: true }) currentPath!: string;
  @Output() newPathEvent = new EventEmitter<string>();
  @Output() onBackEvent = new EventEmitter();

  cols: string[] = ['name', 'size', 'count'];

  constructor() {}
  treeMapClick(event: any) {
    console.log(event);
  }

  goTo(path: string) {
    this.newPathEvent.emit(path);
  }

  goBack() {
    this.onBackEvent.emit();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['directoryList$']) {
      this.directoryList$ = changes['directoryList$'].currentValue;
    }
  }

  formatBytes(bytes: number): string {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;

    while (bytes >= 1024 && i < units.length - 1) {
      bytes /= 1024;
      i++;
    }

    return `${bytes.toFixed(1)} ${units[i]}`;
  }
}
