<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('telephony_outbox', function (Blueprint $table): void {
            $table->timestamp('processing_started_at')->nullable()->after('available_at');
            $table->index(
                ['status', 'processing_started_at', 'id'],
                'telephony_outbox_stale_processing_idx',
            );
        });
    }

    public function down(): void
    {
        Schema::table('telephony_outbox', function (Blueprint $table): void {
            $table->dropIndex('telephony_outbox_stale_processing_idx');
            $table->dropColumn('processing_started_at');
        });
    }
};
