<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('operators', function (Blueprint $table): void {
            $table->index(
                ['available', 'afk', 'reserved_call_id', 'last_call_at', 'id'],
                'operators_allocation_idx',
            );
            $table->index(['reserved_at', 'reserved_call_id'], 'operators_reservation_ttl_idx');
        });

        Schema::table('calls', function (Blueprint $table): void {
            $table->index(
                ['status', 'operator_id', 'id'],
                'calls_assignment_state_idx',
            );
            $table->index(
                ['status', 'next_operator_search_at', 'id'],
                'calls_retry_due_idx',
            );
        });

        Schema::table('telephony_outbox', function (Blueprint $table): void {
            $table->index(
                ['status', 'canceled_at', 'available_at', 'id'],
                'telephony_outbox_claim_due_idx',
            );
            $table->index(
                ['external_call_id', 'type', 'status', 'published_at'],
                'telephony_outbox_assignment_lookup_idx',
            );
        });
    }

    public function down(): void
    {
        Schema::table('telephony_outbox', function (Blueprint $table): void {
            $table->dropIndex('telephony_outbox_assignment_lookup_idx');
            $table->dropIndex('telephony_outbox_claim_due_idx');
        });

        Schema::table('calls', function (Blueprint $table): void {
            $table->dropIndex('calls_retry_due_idx');
            $table->dropIndex('calls_assignment_state_idx');
        });

        Schema::table('operators', function (Blueprint $table): void {
            $table->dropIndex('operators_reservation_ttl_idx');
            $table->dropIndex('operators_allocation_idx');
        });
    }
};
