import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import {
  MatButtonToggleChange,
  MatButtonToggleModule,
} from '@angular/material/button-toggle';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';

@Component({
  selector: 'app-search-bar',
  standalone: true,
  imports: [
    MatCardModule,
    MatFormFieldModule,
    MatInputModule,
    MatButtonModule,
    MatButtonToggleModule,
    FormsModule,
  ],
  templateUrl: './search-bar.component.html',
  styleUrl: './search-bar.component.css',
})
export class SearchBarComponent implements OnChanges {
  @Input({ required: true }) view!: string;
  @Output() onBackEvent = new EventEmitter<void>();
  @Output() onViewChange = new EventEmitter<string>();
  viewToggle: string = this.view;

  onBackClick() {
    this.onBackEvent.emit();
  }

  onViewToggle(newView: MatButtonToggleChange) {
    this.onViewChange.emit(newView.value);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['view']) {
      this.onViewChange.emit(changes['view'].currentValue);
    }
  }
}
