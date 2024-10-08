import { Component } from '@angular/core';
import { MatCardModule } from '@angular/material/card';

@Component({
  selector: 'app-summary-bucket',
  standalone: true,
  imports: [MatCardModule],
  templateUrl: './summary-bucket.component.html',
  styleUrl: './summary-bucket.component.css',
})
export class SummaryBucketComponent {}
