import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';
import { MatCardModule } from '@angular/material/card';
import { ExploreService } from '../services/explore.service';
import { KeyValuePipe, NgFor, NgIf } from '@angular/common';
import { SizePipe } from '../pipes/size.pipe';

@Component({
  selector: 'app-summary',
  standalone: true,
  imports: [MatCardModule, NgFor, KeyValuePipe, NgIf, SizePipe],
  templateUrl: './summary.component.html',
  styleUrl: './summary.component.css',
})
export class SummaryComponent implements OnChanges {
  @Input({ required: true }) path!: string;

  rows = ['standard', 'nearline', 'coldline', 'archive'];
  sizes!: any;
  costs!: any;

  constructor(private exploreService: ExploreService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (
      changes['path'] &&
      changes['path'].currentValue &&
      changes['path'].currentValue !== changes['path'].previousValue
    ) {
      this.fetchSummary();
    }
  }

  async fetchSummary() {
    try {
      const res = await this.exploreService.getSummary(this.path);

      this.sizes = res.size;
      this.costs = res.cost;
    } catch (error) {
      console.error(`Error fetching summary for path ${this.path}: ${error}`);
    }
  }

  capitalize(str: string) {
    return str.charAt(0).toUpperCase() + str.slice(1);
  }
}
