import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SummaryBucketComponent } from './summary-bucket.component';

describe('SummaryBucketComponent', () => {
  let component: SummaryBucketComponent;
  let fixture: ComponentFixture<SummaryBucketComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SummaryBucketComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(SummaryBucketComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
