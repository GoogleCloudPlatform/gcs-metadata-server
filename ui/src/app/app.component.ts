import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { TableComponent } from './viewer/table/table.component';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatButtonModule } from '@angular/material/button';
import { FormsModule } from '@angular/forms';
import { SummaryComponent } from './summary/summary.component';
import { ViewerComponent } from './viewer/viewer.component';
import { SummaryBucketComponent } from './summary-bucket/summary-bucket.component';
import { SearchBarComponent } from './search-bar/search-bar.component';
import { MetadataObject, ExploreService } from './services/explore.service';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    RouterOutlet,
    MatTableModule,
    TableComponent,
    MatButtonToggleModule,
    MatButtonModule,
    FormsModule,
    SummaryComponent,
    ViewerComponent,
    SearchBarComponent,
    SummaryBucketComponent,
    MatProgressSpinnerModule,
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent {
  directoryTitle$!: string;
  directoryList$!: MetadataObject[];

  pathStack: string[] = ['/'];
  view: string = 'directory';

  constructor(private exploreService: ExploreService) {
    this.fetchPath();
  }

  /** fetchPath refreshes directoryList based on last path in pathStack
   */
  async fetchPath() {
    let path = this.pathStack[this.pathStack.length - 1];
    if (!path) return;

    try {
      const result = await this.exploreService.getDir(path, '');
      this.directoryTitle$ = result.title;
      this.directoryList$ = result.contents;
    } catch (error) {
      console.error('Error fetching path:', error);
    }
  }

  onViewChange(newView: string) {
    this.view = newView;
  }

  goTo(newPath: string) {
    const lastPath = this.pathStack[this.pathStack.length - 1];
    if (
      !newPath.endsWith('/') ||
      newPath === lastPath ||
      '/' + newPath === lastPath
    ) {
      return;
    }

    this.pathStack = [...this.pathStack, newPath];
    this.fetchPath();
  }

  goBack() {
    if (this.pathStack.length > 1) {
      this.pathStack = this.pathStack.slice(0, -1);
      this.fetchPath();
    }
  }

  refresh() {
    this.fetchPath();
  }
}
